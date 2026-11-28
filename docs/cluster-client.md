# Cluster client contract (subset)

Janus exposes a **documented Redis Cluster client subset** at the RESP edge so
compatible clients can discover slot ownership and follow redirects. This is not
full Redis Cluster (no gossip, no transparent proxy).

## Slots and hash tags

Keys map to one of **16384** slots with the Redis rule:

`slot(key) = CRC16(hash_tag(key)) % 16384`

Hash tags: when a key contains `{…}` with non-empty content between the first
`{` and the first matching `}`, only that inner byte range is hashed. Keys that
share a tag (for example `{user}a` and `{user}b`) land on the same slot and may
appear together in `MULTI` / multi-key flows.

## Ownership miss → `-MOVED`

The Kernel tracks which slots this process owns and returns a **domain** error
(`ERR slot not owned`) with **no** host:port when a key’s slot is remote.

When composition injects a `janus_protocol::SlotEndpointMap`, the RESP protocol
rewrites that miss into:

```text
-MOVED <slot> <host:port>
```

Clients should update their slot cache and retry against the advertised endpoint.

## Resharding → `-ASK` / `ASKING`

Control plane–directed slot migration uses Redis-compatible transient states on
the Kernel (no host:port in the domain):

| Role | Domain state | Client-visible behavior |
|------|--------------|-------------------------|
| Source | **MIGRATING** (still owns the slot) | Existing keys serve locally; absent keys become `-ASK <slot> <dest>` when an `AskEndpointMap` is configured |
| Destination | **IMPORTING** (not stable owner yet) | Without `ASKING`, ownership misses stay `-MOVED` to the current stable owner; after `ASKING`, the **next** keyed command may run against the importing slot |

```text
ASKING          → +OK  (one-shot per connection)
-ASK <slot> <host:port>
```

Transfer path chosen for this subset: **filtered snapshot export** of keys that
hash to the slot (`Kernel::export_slot` → `import_slot_entries`). Ops / the
control plane copy the payload, then call `finish_migrate_source` /
`finish_migrate_destination` and update the `SlotEndpointMap` (and clear the ASK
destination). After that, redirects are stable `-MOVED` to the new owner.

Without `ASKING` on an IMPORTING node, the policy is Redis-like: treat the slot
as not owned and emit `-MOVED` toward the still-authoritative endpoint from the
slot map.

## Cross-slot

`MULTI` / `EXEC` that queue keys hashing to different slots fail with a stable
RESP simple error (domain text, no host:port):

```text
-CROSSSLOT Keys in request don't hash to the same slot
```

Same-slot commands (including via hash tags) succeed as usual.

## Discovery: `CLUSTER SLOTS`

Surface chosen for this subset: **`CLUSTER SLOTS`** only (not `CLUSTER SHARDS`
or `CLUSTER NODES`).

Reply shape (Redis-compatible nested arrays, masters only — no node id, no
replicas in this subset):

```text
*N
*3
:<start>
:<end>
*2
$… host
:<port>
…
```

The map is composition / control-plane owned. Wire it when building the protocol:

```rust
use std::sync::{Arc, Mutex};
use janus_protocol::{AskEndpointMap, RespProtocol, SlotEndpointMap};
use janus_serializer::RespSerializer;

let map = Arc::new(Mutex::new(SlotEndpointMap::from_ranges([
    (0u16, 8191, "127.0.0.1:7000"),
    (8192, 16383, "127.0.0.1:7001"),
])));
let ask = Arc::new(Mutex::new(AskEndpointMap::new()));
let protocol = RespProtocol::shared(kernel, RespSerializer)
    .with_slot_map(map)
    .with_ask_map(ask);
```

Without a map, `CLUSTER SLOTS` returns `-ERR slot map not configured`, and
ownership misses stay as the domain `-ERR slot not owned` string.

Default single-node processes own every slot; clients never see `-MOVED` until
composition restricts ownership and supplies endpoints.

## Ops procedure (resharding one slot)

1. Authority marks source **MIGRATING** and destination **IMPORTING**; publish
   the ASK destination for that slot.
2. Export slot keys from the source, import on the destination (filtered
   snapshot).
3. Clients that hit absent keys on the source follow `-ASK` → `ASKING` → retry
   once on the destination.
4. Authority finishes both sides, reassigns the slot in `SlotEndpointMap`, and
   clears the ASK destination.
5. Subsequent misses use `-MOVED` to the new owner; `CLUSTER SLOTS` reflects the
   updated map.

## Out of scope

- Automatic rebalance algorithms, cluster gossip (`PFAIL` / `FAIL`), transparent
  proxying, Cap’n Proto as the cluster wire
- Multi-primary / active-active
- Changing CRC16 or the 16384 slot count
