# Single-shard HA operations

Janus can run one writable primary and one (or more) read-only replicas on a
dedicated TCP replica wire. Failover for this topology is **ops-driven**: a
small control-plane supervisor detects a dead primary, promotes a replica, and
publishes the new client endpoint. There is no Raft, etcd, Redis Sentinel wire
protocol, or cluster bus gossip between data nodes.

## Roles

| Role | Client RESP | Replica wire |
|------|-------------|--------------|
| Primary | Accepts writes | Pushes snapshot + records to `--replica-peer` |
| Replica | Rejects writes (`READONLY …`) | Accepts wire on `--replica-bind`, applies snapshot/records |
| After promote | Former replica accepts writes | Stops applying the old primary’s stream |

The Kernel stays membership-blind: write acceptance is a composition flag
(`accepts_writes`). Replica-wire apply bypasses that flag so catch-up still works.

## Config authority

The authority lives **outside** the Kernel. It publishes the current primary
RESP endpoint for clients and operators:

- In-process: `ConfigAuthority::in_memory()`
- On disk: `ConfigAuthority::file(path)` — one line, the endpoint string

After promotion the authority is updated; clients/ops re-point to the new
endpoint. Data nodes do not elect a leader among themselves.

## Failover sequence

1. Start replica (`--role replica`), then primary (`--role primary`).
2. Seed the authority with the primary’s RESP bind address.
3. Supervisor probes the primary endpoint with a TCP connect timeout.
4. When the primary stays unreachable for a configured `dead_after` window,
   call `promote_replica` / `promote_ha_replica`:
   - stop applying the remote stream
   - set `accepts_writes = true` on the chosen replica
   - publish the replica’s RESP address as the new primary
5. Point clients at the authority’s endpoint.

Library entry points: `janus::serve_replica_ha`, `janus::promote_ha_replica`,
`janus_replication::{detect_and_promote, FailoverConfig, ConfigAuthority}`.

## Rejoining a former primary

A returning (old) primary must **not** resume writes automatically. Keep it
out of service or start it as a **replica** (`accepts_writes = false`) and
full-sync from the new primary via the existing bootstrap path (domain
snapshot + catch-up records). Partial sync / backlog offsets are out of scope.

## Loss window

Replication is asynchronous. Writes acknowledged on the old primary that had
not yet reached the replica can be lost across failover. There is no `WAIT`
or semi-sync in this milestone.

## Out of scope

- Raft / Paxos / embedded etcd
- Redis Cluster gossip (`PFAIL` / `FAIL`)
- Redis Sentinel protocol compatibility
- `-ASK` / live slot migration (see [cluster-client.md](cluster-client.md) for the
  `-MOVED` + `CLUSTER SLOTS` subset already available at the RESP edge)
- TLS between nodes
- Mandatory separate control-plane process (in-process supervisor or harness is enough)
