# Glossary

> This document defines the common language used throughout the Janus project.

---

# Kernel

The central execution engine of Janus.

The kernel receives normalized commands and coordinates their execution.

The kernel is independent of networking protocols and storage implementations.

---

# Command

An internal representation of an operation requested by a client.

Examples:

- Get
- Set
- Delete
- Exists

Commands are protocol-agnostic.

---

# Response

The result produced after a command is executed.

Responses are converted back into protocol-specific formats before being
returned to the client.

---

# Protocol

A communication format used by external clients.

Examples:

- RESP
- HTTP
- gRPC

Protocols translate requests into Commands.

Protocols never execute business logic.

---

# Client

Any external application interacting with Janus.

Examples include:

- Redis CLI
- Web applications
- SDKs
- Services

---

# Router

Component responsible for dispatching Commands to the Kernel.

The router isolates protocol implementations from execution logic.

---

# Storage Engine

A component responsible for reading and writing data.

Different storage engines may implement different persistence strategies.

Examples:

- Memory
- RocksDB
- Redb

---

# Cache Engine

Responsible for cache-specific behavior.

Examples include:

- Expiration
- Eviction
- Cache policies

---

# Persistence

Any mechanism used to make data durable.

Examples include:

- Snapshot
- WAL
- Append-only log

---

# Entry

A single stored record.

An Entry generally contains:

- Key
- Value
- Metadata
- Expiration information

---

# Key

The unique identifier of an Entry.

---

# Value

The data associated with a Key.

Janus initially treats values as opaque binary data.

Higher-level protocols may interpret those bytes differently.

---

# Namespace

A logical separation of data.

Namespaces allow independent groups of keys without requiring multiple
servers.

---

# Database

A collection of namespaces and their associated configuration.

Future versions may support multiple databases.

---

# Module

An independently replaceable subsystem.

Examples:

- Protocol module
- Storage module
- Replication module

Modules communicate through stable interfaces.

---

# Plugin

An optional module extending Janus functionality without modifying the
Kernel.

---

# Trait

A Rust abstraction defining shared behavior.

Traits are the primary mechanism used to decouple implementations.

---

# Event

A notification emitted by the Kernel describing something that happened.

Examples:

- KeyCreated
- KeyUpdated
- KeyDeleted

Future modules may subscribe to these events.

---

# Replica

A copy of data maintained on another node.

Replication is intended for future distributed versions.

---

# Shard

A partition of the keyspace.

Each shard owns a subset of all stored keys.

---

# Cluster

A group of Janus nodes working together.

Clusters distribute data while presenting a unified interface to clients.

---

# Adapter

A component translating one interface into another.

Examples:

- RESP Adapter
- HTTP Adapter

Adapters isolate external systems from the Kernel.

---

# Benchmark

A repeatable performance measurement.

Benchmarks are used to validate optimizations before architectural changes.

---

# ADR

Architecture Decision Record.

A document describing an important architectural decision,
its motivation and consequences.

Every significant design decision should be documented as an ADR.