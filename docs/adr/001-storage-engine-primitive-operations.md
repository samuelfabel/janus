# ADR-001: StorageEngine Trait Scope Limitation to Primitive Operations

## Status
Proposed

## Context
The `StorageEngine` trait requires a clear and well-defined scope. There is often a tendency to introduce high-level convenience methods or orchestrational behaviors directly into the storage interface. Mixing basic data access with complex or high-level workflows bloats the trait interface, increases implementation complexity for new backends, and fragments the core system logic across mismatched architectural boundaries.

## Decision
We will restrict the `StorageEngine` trait to expose **only primitive storage operations** (e.g., core `get`, `set`, `delete`).

Higher-level behaviors, orchestrations, and composite operations (such as `exists`, `mget`, and `mset`) belong strictly within the `Kernel` layer.

**Exception Clause:** A high-level or composite operation may only be moved down into the `StorageEngine` trait if there is a measured, documented, and significant performance justification backed by concrete benchmark data (e.g., avoiding severe network round-trips or serialization overheads that measurably degrade system performance).

## Consequences

### Positive (Benefits)
* **Lean Interface:** The `StorageEngine` trait remains simple, cohesive, and easy to understand.
* **Ease of Implementation:** Creating or modifying backend implementations (e.g., In-Memory, RocksDB, Mock engines for testing) becomes significantly easier due to the minimal surface area.
* **Centralized Business Logic:** The `Kernel` retains explicit control and visibility over how data flows, validates, and coordinates, keeping the architecture predictable.

### Negative (Trade-offs)
* **Potential Initial Overhead:** Composite actions executed via the `Kernel` (e.g., implementing an unoptimized `mget` via sequential, individual primitive `get` requests) could be less efficient initially until a proven performance bottleneck warrants specific engine-level optimization.