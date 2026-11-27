# Cluster client contract (subset)

Janus exposes a **documented Redis Cluster client subset** at the RESP edge so
compatible clients can discover slot ownership and follow redirects. This is not
full Redis Cluster (no gossip, no proxy, no resharding yet).

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
use std::sync::Arc;
use janus_protocol::{RespProtocol, SlotEndpointMap};
use janus_serializer::RespSerializer;

let map = Arc::new(SlotEndpointMap::from_ranges([
    (0u16, 8191, "127.0.0.1:7000"),
    (8192, 16383, "127.0.0.1:7001"),
]));
let protocol = RespProtocol::shared(kernel, RespSerializer).with_slot_map(map);
```

Without a map, `CLUSTER SLOTS` returns `-ERR slot map not configured`, and
ownership misses stay as the domain `-ERR slot not owned` string.

Default single-node processes own every slot; clients never see `-MOVED` until
composition restricts ownership and supplies endpoints.

## Out of scope (later)

- `-ASK` / `ASKING`, `MIGRATING` / `IMPORTING`, slot migration — future work
- Cluster gossip (`PFAIL` / `FAIL`), transparent proxying, Cap’n Proto as the
  cluster wire
- Changing CRC16 or the 16384 slot count
