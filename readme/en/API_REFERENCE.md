# API Reference

Complete API specification for AkkaraDB.

## 📚 Table of Contents

- [Typed API (AkkDSL)](#typed-api-akkdsl)
- [Low-level API](#low-level-api)
- [Startup Modes](#startup-modes)
- [Options Configuration](#options-configuration)
- [Data Types](#data-types)
- [Query API](#query-api)

---

## Typed API (AkkDSL)

A type-safe Kotlin DSL API. Through the Kotlin compiler plugin, data classes can be used directly as key-value pairs.

### Opening a Database

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.StartupMode
import dev.swiftstorm.akkaradb.common.ShortUUID
import dev.swiftstorm.akkaradb.engine.Id
import java.nio.file.Paths

data class User(
    @Id val id: ShortUUID,
    val name: String,
    val age: Int
)

val base = Paths.get("./data/akkdb")
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.NORMAL)
```

**Signature:**

```kotlin
inline fun <reified T : Any, reified ID : Any> open(
    baseDir: Path,
    configure: AkkDSLCfgBuilder.() -> Unit = {}
): PackedTable<T, ID>
```

```kotlin
inline fun <reified T : Any, reified ID : Any> open(
    baseDir: Path,
    mode: StartupMode,
    noinline customize: AkkDSLCfgBuilder.() -> Unit = {}
): PackedTable<T, ID>
```

```kotlin
inline fun <reified T : Any, reified ID : Any> open(
    cfg: AkkDSLCfg
): PackedTable<T, ID>
```

### Basic Operations

#### put - Writing Data

```kotlin
db.put(id: ID, entity: T)
// or
db.put(entity: T)
```

**Example:**

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.NORMAL)
val id = ShortUUID.generate()
val user = User(id, "Taro", 25)

// Write with ID and value
db.put(id, user)

// Automatically extract ID from property with @Id annotation
db.put(user)
```

**Return Value:** None (void)

**Note:**

- Unlike the low-level API `put()` which returns a global sequence number, the Typed API `put()` has no return value
- If you need the sequence number, access it via `db.akkdb.lastSeq()`

---

#### get - Reading Data

```kotlin
val value: T? = db.get(id: ID)
```

**Example:**

```kotlin
val id = ShortUUID.generate()
val user = db.get(id)
if (user != null) {
    println("User found: $user")
} else {
    println("User does not exist or has been deleted")
}
```

**Return Value:** `T` if the value exists, `null` if it doesn't exist or is a tombstone

---

#### delete - Deleting Data

```kotlin
db.delete(id: ID)
```

**Example:**

```kotlin
val id = ShortUUID.generate()
db.delete(id)
println("Deletion complete")
```

**Return Value:** None (void)

---

#### upsert - Updating or Inserting Data

```kotlin
val entity: T = db.upsert(id: ID, init: T.() -> Unit)
```

**Example:**

```kotlin
// Update if ID exists, create new if it doesn't
val user = db.upsert(id) {
    name = "Jiro"
    age = 30
}
println("Upsert complete: $user")
```

**Return Value:** The updated or created entity

**Note:**

- If the entity doesn't exist, a no-argument constructor is required
- Modify existing properties in the `init` lambda

---

#### close - Closing the Database

```kotlin
db.close()
```

Flushes all changes and releases resources.

---

## Low-level API

Direct operation API using `ByteBufferL`. You need to manage serialization yourself.

### Opening a Database

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import java.nio.file.Paths

val db = AkkaraDB.open(
    AkkaraDB.Options(baseDir = Paths.get("./data/akkdb"))
)
```

### Basic Operations

#### put - Writing Key-Value

```kotlin
fun put(key: ByteBufferL, value: ByteBufferL): Long
```

**Example:**

```kotlin
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.charset.StandardCharsets

val key = ByteBufferL.wrap(
    StandardCharsets.UTF_8.encode("mykey")
).position(0)

val value = ByteBufferL.wrap(
    StandardCharsets.UTF_8.encode("myvalue")
).position(0)

val seq = db.put(key, value)
println("Write complete: seq=$seq")
```

**Return Value:** Global sequence number

**Internal Operation:**

1. Get a new sequence number via `mem.nextSeq()`
2. Write `WalOp.Add` to WAL (durable before apply)
3. Write to MemTable
4. Return the sequence number

---

#### get - Reading Value from Key

```kotlin
fun get(key: ByteBufferL): ByteBufferL?
```

**Example:**

```kotlin
val result = db.get(key)
if (result != null) {
    val str = StandardCharsets.UTF_8.decode(result.rawDuplicate()).toString()
    println("Value: $str")
}
```

**Return Value:** `ByteBufferL` if the value exists, `null` if it doesn't exist or is a tombstone

**Internal Operation:**

1. Search MemTable (fast path)
2. If not found, search SSTables from newest to oldest
3. If `useStripeForRead=true`, fallback search from Stripe

---

#### delete - Deleting Key

```kotlin
fun delete(key: ByteBufferL): Long
```

**Example:**

```kotlin
val seq = db.delete(key)
println("Deletion complete: seq=$seq")
```

**Return Value:** Sequence number at deletion

**Internal Operation:**

1. Get a new sequence number via `mem.nextSeq()`
2. Write `WalOp.Delete` (with tombstone flag) to WAL
3. Insert tombstone into MemTable

---

#### compareAndSwap - Conditional Update

```kotlin
fun compareAndSwap(
    key: ByteBufferL,
    expectedSeq: Long,
    newValue: ByteBufferL?
): Boolean
```

**Example:**

```kotlin
val seq1 = db.put(key, value)

// Update only if seq1 matches
val success = db.compareAndSwap(key, expectedSeq = seq1, newValue = newValue)

if (success) {
    println("Update succeeded")
} else {
    println("Update failed (conflict occurred)")
}

// To delete, specify null for newValue
db.compareAndSwap(key, expectedSeq = seq1, newValue = null)
```

**Return Value:** `true` on success, `false` if expectedSeq doesn't match

**Internal Operation:**

1. Execute `compareAndSwap()` in MemTable
2. If successful and `durableCas=true`, write to WAL (idempotent)
3. Return success/failure

---

#### range - Range Query

```kotlin
fun range(
    start: ByteBufferL,
    end: ByteBufferL
): Sequence<MemRecord>
```

**Example:**

```kotlin
val startKey = ByteBufferL.wrap(StandardCharsets.UTF_8.encode("key:0000")).position(0)
val endKey = ByteBufferL.wrap(StandardCharsets.UTF_8.encode("key:9999")).position(0)

for (record in db.range(startKey, endKey)) {
    println("Key: ${record.key}, Value: ${record.value}, Seq: ${record.seq}")
}
```

**Return Value:** Sequence of `MemRecord`

**Internal Operation:**

1. Get iterators from MemTable and SSTables
2. K-way merge sorted by key order
3. Prioritize newest (maximum seq) if multiple versions of same key exist
4. Skip tombstones

**Note:**

- `end` is exclusive (not included)
- Be mindful of memory with large datasets

---

#### flush - Force Flush

```kotlin
fun flush()
```

Write MemTable to SSTable, seal Stripe, and record checkpoint to Manifest.

**Example:**

```kotlin
db.put(key, value)
db.flush() // Explicit flush
```

---

#### close - Closing the Database

```kotlin
fun close()
```

Calls `flush()` then releases all resources.

---

## Custom Serialization

### Overview

AkkaraDB automatically serializes standard Kotlin data classes, but you can register custom `TypeAdapter`s for types from external libraries or types requiring
special serialization logic.

### TypeAdapter Interface

```kotlin
interface TypeAdapter<T> {
    /** Estimated size after serialization (bytes). Should return an upper bound. */
    fun estimateSize(value: T): Int

    /** Serialize value to buffer. Advance buffer.position. */
    fun write(value: T, buffer: ByteBufferL)

    /** Deserialize value from buffer. Advance buffer.position. */
    fun read(buffer: ByteBufferL): T

    /** Deep copy (default implementation: encode→decode) */
    fun copy(value: T): T
}
```

### Registering Adapters

```kotlin
import dev.swiftstorm.akkaradb.common.binpack.AdapterRegistry

// Method 1: Register with reified type parameter (recommended)
AdapterRegistry.register<Component>(componentAdapter)

// Method 2: Register with KClass
AdapterRegistry.registerAdapter(Component::class, componentAdapter)

// Method 3: Register with KType (advanced use)
AdapterRegistry.registerAdapter(typeOf<Component>(), componentAdapter)
```

### Implementation Example: External Library Type

```kotlin
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import dev.swiftstorm.akkaradb.common.binpack.AdapterRegistry

// Example adapter for Minecraft Adventure Component
private val componentAdapter = object : TypeAdapter<Component> {
    private val serializer = GsonComponentSerializer.gson()

    override fun estimateSize(value: Component): Int {
        val json = serializer.serialize(value)
        return 4 + json.length  // 4 bytes for size + JSON string
    }

    override fun write(value: Component, buffer: ByteBufferL) {
        val json = serializer.serialize(value)
        val bytes = json.toByteArray(Charsets.UTF_8)

        buffer.i32 = bytes.size
        buffer.putBytes(bytes)
    }

    override fun read(buffer: ByteBufferL): Component {
        val size = buffer.i32
        require(size >= 0) { "Negative size: $size" }
        require(buffer.remaining >= size) {
            "Insufficient bytes: need=$size remaining=${buffer.remaining}"
        }

        val bytes = ByteArray(size)
        repeat(size) { i ->
            bytes[i] = buffer.i8.toByte()
        }

        val json = String(bytes, Charsets.UTF_8)
        return serializer.deserialize(json)
    }
}

// Register
AdapterRegistry.register<Component>(componentAdapter)
```

### Common Mistakes and Solutions

#### 1. Buffer Position Not Advanced

```kotlin
// ✓ Correct: Advance position
override fun write(value: String, buffer: ByteBufferL) {
    buffer.i32 = value.length  // position += 4
    buffer.putBytes(value.toByteArray())  // position += bytes.size
}

// ✗ Wrong: Position not advanced
override fun write(value: String, buffer: ByteBufferL) {
    buffer.at(0).i32 = value.length  // Absolute access (position doesn't advance)
}
```

**Point:** `write` and `read` must always advance `buffer.position`. Use relative access (`buffer.i32`, `buffer.putBytes`, etc.).

#### 2. Registration Timing

```kotlin
// ✓ Correct: Register before opening database
AdapterRegistry.register<Component>(componentAdapter)
val db = AkkDSL.open<ChatMessage, String>(base, StartupMode.NORMAL)

// ✗ Wrong: Register after opening database
val db = AkkDSL.open<ChatMessage, String>(base, StartupMode.NORMAL)
AdapterRegistry.register<Component>(componentAdapter)  // Too late
```

**Point:** Adapters must be registered before the first serialization/deserialization.

#### 3. Thread Safety

```kotlin
// ✓ Correct: Immutable or thread-safe
private val componentAdapter = object : TypeAdapter<Component> {
    private val serializer = GsonComponentSerializer.gson()  // immutable
    // ...
}

// ✗ Wrong: Mutable state
private val badAdapter = object : TypeAdapter<SomeType> {
    private var counter = 0  // Not thread-safe
    override fun write(value: SomeType, buffer: ByteBufferL) {
        counter++  // Dangerous
    }
}
```

**Point:** `TypeAdapter` implementations must be thread-safe.

### Adapter Management API

```kotlin
// Check if adapter exists
val hasAdapter = AdapterRegistry.hasCustomAdapter(Component::class)

// Unregister adapter
AdapterRegistry.unregisterAdapter(Component::class)

// Clear all custom adapters
AdapterRegistry.clearAll()

// Get statistics
val stats = AdapterRegistry.getStats()
println("Custom adapters: ${stats.customAdapterCount}")
```

### Default Supported Types

The following types are supported by default and don't require custom adapters:

**Primitive Types:**

- `Int`, `Long`, `Short`, `Byte`, `Boolean`, `Float`, `Double`, `Char`

**Standard Types:**

- `String`, `ByteArray`
- `UUID`, `BigInteger`, `BigDecimal`
- `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`

**Collections:**

- `List<T>`, `Set<T>`, `Map<K, V>`

**Other:**

- `Enum` (any Enum type)
- `Nullable<T>` (any nullable type)
- Kotlin data classes (automatic reflection)

---

## Startup Modes

In the Typed API, you can choose a startup mode depending on your use case.

### StartupMode.NORMAL (Recommended)

Balanced configuration. Suitable for most use cases.

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.NORMAL)
```

**Configuration:**

- `k = 4, m = 2`
- `walFastMode = true`
- `walGroupN = 64`
- `walGroupMicros = 1_000`
- `flushMaxBlocks = 64`
- `flushMaxMicros = 1_000`
- `parityCoder = RSParityCoder(2)`

**Characteristics:**

- Write Latency: Moderate
- Durability: High (WAL group commit)

---

### StartupMode.FAST

Write speed priority. Durability is slightly reduced.

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.FAST)
```

**Configuration:**

- `k = 4, m = 1`
- `walFastMode = true`
- `walGroupN = 256`
- `walGroupMicros = 12_000`
- `flushMaxBlocks = 256`
- `flushMaxMicros = 2_000`
- `parityCoder = RSParityCoder(1)`

**Characteristics:**

- Write Latency: Low
- Durability: Moderate (up to 12ms delay)

---

### StartupMode.DURABLE

Durability priority. Write speed is reduced.

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.DURABLE)
```

**Configuration:**

- `k = 4, m = 2`
- `walFastMode = false`
- `walGroupN = 1`
- `walGroupMicros = 0`
- `flushMaxBlocks = 32`
- `flushMaxMicros = 500`
- `parityCoder = RSParityCoder(2)`
- `durableCas = true`

**Characteristics:**

- Write Latency: High
- Durability: Maximum (immediate fsync)

---

### StartupMode.ULTRA_FAST

For testing. Minimizes fsync. **Not recommended for production.**

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.ULTRA_FAST)
```

**Configuration:**

- `k = 4, m = 1`
- `walFastMode = true`
- `walGroupN = 512`
- `walGroupMicros = 50_000`
- `flushMaxBlocks = 512`
- `flushMaxMicros = 50_000`
- `parityCoder = RSParityCoder(1)`

**Characteristics:**

- Write Latency: Very low
- Durability: Low (potential data loss on crash)

---

## Options Configuration

The low-level API allows detailed tuning.

### AkkaraDB.Options

```kotlin
data class Options(
    val baseDir: Path,
    val k: Int = 4,
    val m: Int = 2,
    val flushPolicy: FlushPolicy = FlushPolicy(
        maxBlocks = 32,
        maxMicros = 500
    ),
    val walFastMode: Boolean = true,
    val stripeFastMode: Boolean = true,
    val walGroupN: Int = 64,
    val walGroupMicros: Long = 1_000,
    val parityCoder: ParityCoder? = null,
    val durableCas: Boolean = false,
    val useStripeForRead: Boolean = false,
    val bloomFPRate: Double = 0.01,
    val debug: Boolean = false
)
```

### Parameter Details

#### baseDir

Root directory for the database. The following files/directories are created:

- `wal.akwal` - Write-Ahead Log
- `manifest.akmf` - Manifest file
- `sst/` - SSTable files (L0, L1, ...)
- `lanes/` - Stripe lane files (data_0, data_1, ..., parity_0, ...)

#### k (Data Lanes)

Number of data lanes in Stripe. 4 is normally recommended.

**Tuning Guide:**

- `k = 4`: Balanced (recommended)
- `k = 8`: High throughput (wide write bandwidth)
- `k = 2`: Low latency priority

#### m (Parity Lanes)

Number of parity lanes in Stripe. Determines the level of redundancy.

**Settings:**

- `m = 0`: No parity (no redundancy)
- `m = 1`: Reed-Solomon parity (recover from 1 lane failure)
- `m = 2`: Reed-Solomon parity (recover from 2 lane failures)
- `m ≥ 3`: Reed-Solomon parity (recover from m lane failures)

**Recommendations:**

- Normal: `m = 2` (Reed-Solomon)
- High reliability: `m ≥ 3` (Reed-Solomon)
- Testing: `m = 0` (no parity)

#### walGroupN

WAL group commit count. Fsync is issued when this many entries accumulate or `walGroupMicros` elapses.

**Tuning Guide:**

- Small values (1-32): Low latency, low throughput
- Middle values (64-256): Balanced
- Large values (512-1024): High throughput, high latency

#### walGroupMicros

WAL group commit time (microseconds).

**Tuning Guide:**

- `0-1_000 µs`: Low latency
- `1_000-12_000 µs`: Balanced
- `12_000-50_000 µs`: High throughput (maximum delay)

#### walFastMode

If `true`, use `force(false)` (equivalent to fdatasync). If `false`, use `force(true)` (equivalent to fsync).

**Recommendation:** `true` in most cases

#### stripeFastMode

If `true`, Stripe fsync becomes asynchronous.

**Recommendation:** `true` in most cases

#### durableCas

If `true`, write to WAL on CAS operations as well.

**Recommendation:** `false` (default). Use `true` if durability is required.

#### useStripeForRead

If `true`, fallback read from Stripe if not found in MemTable and SST.

**Recommendation:** `false` (performance priority). Use `true` for debugging only.

---

## Data Types

### MemRecord

Record type returned by `range()` in low-level API.

```kotlin
data class MemRecord(
    val key: ByteBufferL,
    val value: ByteBufferL,
    val seq: Long,
    val flags: Byte,
    val keyHash: Int,
    val approxSizeBytes: Int
) {
    val tombstone: Boolean
        get() = (flags.toInt() and 0x01) != 0

    companion object {
        val EMPTY: ByteBufferL = ByteBufferL.allocate(0)
    }
}
```

### ByteBufferL

AkkaraDB-specific ByteBuffer extension type. Supports zero-copy operations.

**Main Methods:**

```kotlin
// Creation
fun allocate(size: Int, direct: Boolean = true): ByteBufferL
fun wrap(buffer: ByteBuffer): ByteBufferL

// Position operations
fun position(newPosition: Int): ByteBufferL
fun limit(newLimit: Int): ByteBufferL
fun at(offset: Int): ByteBufferL

// Reading (Little Endian)
val i8: Int          // Read as u8 (0-255)
val i16: Int         // Read as i16
val i32: Int         // Read as i32
val i64: Long        // Read as i64

// Writing (Little Endian)
fun put(src: ByteBufferL)
fun putBytes(bytes: ByteArray)

// Utilities
fun duplicate(): ByteBufferL
fun asReadOnlyDuplicate(): ByteBufferL
fun rawDuplicate(): ByteBuffer
val remaining: Int
```

**Usage Example:**

```kotlin
val buf = ByteBufferL.allocate(1024)
buf.at(0).i32 = 42
val value = buf.at(0).i32
```

---

## Query API

Type-safe Query API is available through the Kotlin compiler plugin.

### query - Building a Query

```kotlin
fun query(
    @AkkQueryDsl block: T.() -> Boolean
): AkkQuery
```

**Note:** This method is rewritten to `akkQuery()` by the compiler plugin. If the plugin is not applied, it will error.

### runQ - Executing a Query

```kotlin
fun runQ(query: AkkQuery): Sequence<T>
```

**Example:**

```kotlin
val users = db.runQ(db.query { age > 18 && name.startsWith("Taro") })
for (user in users) {
    println(user)
}
```

### Convenience Methods

```kotlin
// Get as list
fun runToList(@AkkQueryDsl block: T.() -> Boolean): List<T>

// Get first element
fun firstOrNull(@AkkQueryDsl block: T.() -> Boolean): T?

// Check existence
fun exists(@AkkQueryDsl block: T.() -> Boolean): Boolean

// Count
fun count(@AkkQueryDsl block: T.() -> Boolean): Int
```

**Examples:**

```kotlin
// List of users older than 18
val adults = db.runToList { age > 18 }

// Get first user named "Taro"
val taro = db.firstOrNull { name == "Taro" }

// Check if users aged 30+ exist
val hasOldUsers = db.exists { age >= 30 }

// Count users in their 20s
val twenties = db.count { age in 20..29 }
```

### Supported Operators

- Comparison operators: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Logical operators: `&&`, `||`, `!`
- Null check: `x == null`, `x != null`
- Collections: `x in collection`, `x !in collection`

**Internal Operation:**

1. Compiler plugin converts lambda to `AkkExpr` AST representation
2. `runQ()` fetches keys in namespace range with `db.range()`
3. Deserialize each entity and evaluate predicate
4. Return only elements matching the condition

---

Next: [Architecture](./ARCHITECTURE.md) | [Benchmarks](./BENCHMARKS.md)

[Back to Overview](./ABOUT.md)

---