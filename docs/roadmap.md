# Roadmap

> Janus is developed incrementally.
>
> Every milestone introduces new architectural concepts while keeping the
> project functional at every stage.

---

# Guiding Principles

- Every milestone must produce a working system.
- Every milestone should introduce a new engineering concept.
- Features are added only when the underlying architecture is ready.
- Learning is prioritized over feature count.

---

# Phase 0 — Foundation

## Goals

- Repository structure
- Cargo Workspace
- Documentation
- Project conventions

## Learning Objectives

- Cargo Workspaces
- Crate organization
- Project architecture
- Documentation

---

# Phase 1 — Core Domain

## Goals

Implement the internal execution model.

Examples:

- Commands
- Responses
- Errors
- Traits
- Kernel abstraction

## Learning Objectives

- Ownership
- Traits
- Enums
- Pattern Matching
- Error handling

---

# Phase 2 — In-Memory Storage

## Goals

Create the first storage engine.

Examples:

- HashMap
- CRUD operations

## Learning Objectives

- Collections
- Generics
- Interior mutability
- Borrow checker

---

# Phase 3 — Networking

## Goals

Accept TCP connections.

Examples:

- TCP Server
- Request handling

## Learning Objectives

- std::net
- Threads
- Blocking I/O

---

# Phase 4 — RESP Protocol

## Goals

Become compatible with basic Redis clients.

Examples:

- RESP parser
- Serialization
- Command translation

## Learning Objectives

- Parsers
- Buffers
- Binary protocols

---

# Phase 5 — Expiration

## Goals

Support key expiration.

Examples:

- TTL
- Background cleanup

## Learning Objectives

- Time
- Scheduling
- Data lifecycle

---

# Phase 6 — Persistence

## Goals

Store data on disk.

Examples:

- Snapshot
- WAL

## Learning Objectives

- Files
- Serialization
- Recovery

---

# Phase 7 — Concurrency

## Goals

Support concurrent access safely.

Examples:

- Shared storage
- Locks

## Learning Objectives

- Arc
- Mutex
- RwLock
- Thread safety

---

# Phase 8 — Async Runtime

## Goals

Replace blocking networking.

Examples:

- Tokio
- Async networking

## Learning Objectives

- Futures
- Async/Await
- Executors

---

# Phase 9 — Modular Architecture

## Goals

Allow independent modules.

Examples:

- Storage plugins
- Protocol plugins
- Cache policies

## Learning Objectives

- Dynamic dispatch
- Plugin architecture
- Dependency inversion

---

# Phase 10 — Distributed Systems

## Goals

Run Janus across multiple nodes.

Examples:

- Cluster
- Replication

## Learning Objectives

- Consensus
- Replication
- Sharding
- Failure recovery

---

# Future Topics

The following topics may be explored after the initial architecture is
stable.

- Transactions
- Metrics
- Tracing
- Authentication
- Authorization
- SQL layer
- Compression
- Encryption
- Observability
- Benchmark suite

---

# Success Criteria

The project is successful when:

- Every module remains understandable.
- Every architectural decision is documented.
- Performance improvements are measurable.
- New features can be added with minimal coupling.
- The codebase continues to be an educational reference.