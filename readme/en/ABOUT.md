# About AkkaraDB

AkkaraDB is an **ultra-low latency** embedded key-value store running on the JVM. Implemented in Kotlin, it provides predictable tail latency and a simple
operational model.

## 🎯 Key Features

### ⚡ Ultra-Low Latency

- **P99 Write Latency**: ≤ 60 µs (with WAL durability)
- **P99 Read Latency**: ≈ 12 µs (on memory cache hit)
- NVMe SSD optimization for high-speed I/O

### 🛡️ Crash-Safe

- **WAL (Write-Ahead Log)**: All writes are logged before durability
- **Manifest**: System state managed through append-only logs
- **Stripe Redundancy**: Optional data protection with k+m parity (XOR, Reed-Solomon support)

### 📦 Zero Dependencies

- No external runtime dependencies (JDK + Kotlin only)
- Lightweight design suitable for embedding
- Optional Prometheus metrics support

### 🔧 Flexible API

- **Low-level API**: Direct manipulation via `ByteBufferL`
- **Typed DSL**: Type-safe queries through Kotlin compiler plugin
- **Java Compatibility**: Future support for JDBC DataSource and Criteria API

## 🏗️ Architecture Overview

AkkaraDB consists of the following components:

```
┌─────────────────────────────────────────┐
│           Typed API (AkkDSL)            │  ← Kotlin compiler plugin
├─────────────────────────────────────────┤
│          Low-level Engine API           │  ← put/get/delete/CAS
├──────────┬──────────┬───────────────────┤
│ MemTable │   WAL    │   SSTable (LSM)   │
├──────────┴──────────┴───────────────────┤
│      Stripe (k+m Parity Lanes)          │  ← Optional redundancy
├─────────────────────────────────────────┤
│    Block Format (AKHdr32 + Payload)     │
└─────────────────────────────────────────┘
```

### Data Flow

1. **Write**: `put/delete` → WAL write (durability) → MemTable update → ACK
2. **Read**: `get` → MemTable lookup → SSTable lookup (using Bloom filter) → Stripe fallback (optional)
3. **Flush**: MemTable exceeds threshold → Write to L0 SST → Compaction
4. **Recovery**: Load Manifest → Replay WAL → Validate Stripe

For details, see [Architecture](./ARCHITECTURE.md).

## 🎓 Design Principles

### Core Invariants

1. **Global Sequence**: u64 monotonically increasing, unique across entire DB
2. **Replacement Rules**:
    - Higher seq wins
    - On seq equality, tombstone wins (no resurrection)
3. **Key Order**: Byte-wise lexicographic order
4. **Durability Boundary**: Write ACK = WAL durability complete
5. **Recovery Guarantee**: last durable WAL ≤ last sealed manifest

### Performance Tuning Strategy

- **Write Tail Improvement**: Increase `walGroupN` (batching effect)
- **Read Tail Improvement**: Pin indexes/Bloom to memory, adjust page cache
- **Compaction Stalls**: Adjust L0 threshold, add compaction resources

## 📊 Use Cases

### Suitable For

- Real-time applications requiring low latency
- JVM applications needing embedded databases
- Transaction logs, session stores
- Cache layer with persistence

### Not Suitable For

- Cases requiring distributed consensus (currently single-node only)
- Complex secondary indexes required
- Default encryption required

## 🔮 Future Plans

- [ ] Java compatibility layer (JDBC DataSource, Criteria API)
- [ ] 24-hour stability testing
- [ ] Crash injection testing
- [ ] WebGUI debugger tool
- [ ] Distributed replication (future v4+)

---

Next Steps: [Installation](./INSTALLATION.md) | [Quick Start](./QUICKSTART.md)

---