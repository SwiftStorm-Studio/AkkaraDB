# Quick Start

A guide to get started with AkkaraDB in 5 minutes.

## 📝 Prerequisites

- [Installation](./INSTALLATION.md) is complete
- JDK 17 or higher is installed

## 🚀 Basic Usage

### Typed API (Recommended)

Type-safe API using Kotlin data classes:

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.StartupMode
import dev.swiftstorm.akkaradb.engine.Id
import java.nio.file.Paths

// 1. Define data model (@Id annotation marks primary key)
data class User(
    @Id val id: String,
    val name: String,
    val age: Int,
    val email: String
)

fun main() {
    // 2. Open database (specify entity type and ID type)
    val base = Paths.get("./data/akkdb")
    val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

    // 3. Write data
    // Method 1: Specify ID and entity separately
    users.put(
        "user001",
        User(
            id = "user001",
            name = "Taro Yamada",
            age = 28,
            email = "yamada@example.com"
        )
    )

    // Method 2: Auto-extract @Id from entity
    users.put(
        User(
            id = "user002",
            name = "Hanako Sato",
            age = 25,
            email = "sato@example.com"
        )
    )

    println("Write complete")

    // 4. Read data
    val user = users.get("user001")
    println("Read result: $user")

    // 5. Delete data
    users.delete("user001")
    println("Delete complete")

    // 6. Close database
    users.close()
}
```

---

### Low-level API

Direct manipulation using `ByteBufferL` (for advanced use):

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

fun main() {
    // 1. Open database
    val base = Paths.get("./data/akkdb")
    val db = AkkaraDB.open(
        AkkaraDB.Options(baseDir = base)
    )

    // 2. Prepare key and value
    val key = ByteBufferL.wrap(
        StandardCharsets.UTF_8.encode("hello")
    ).position(0)

    val value = ByteBufferL.wrap(
        StandardCharsets.UTF_8.encode("world")
    ).position(0)

    // 3. Write (returns sequence number)
    val seq = db.put(key, value)
    println("Write complete (seq=$seq)")

    // 4. Read
    val result = db.get(key)
    if (result != null) {
        val str = StandardCharsets.UTF_8.decode(result.rawDuplicate()).toString()
        println("Read result: $str")
    }

    // 5. Delete
    db.delete(key)
    println("Delete complete")

    // 6. Flush and close
    db.flush()
    db.close()
}
```

---

## 🎛️ Startup Modes

Choose a startup mode depending on your use case:

```kotlin
// Balanced (recommended)
// k=4, m=2, walGroupN=64, walGroupMicros=1000
val db = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// Fast writes (durability slightly reduced)
// k=4, m=1, walGroupN=256, walGroupMicros=12000
val db = AkkDSL.open<User, String>(base, StartupMode.FAST)

// High durability (write speed reduced)
// walGroupN=1, walGroupMicros=0, durableCas=true
val db = AkkDSL.open<User, String>(base, StartupMode.DURABLE)

// Ultra fast (testing only, minimal fsync)
// walGroupN=512, walGroupMicros=50000
val db = AkkDSL.open<User, String>(base, StartupMode.ULTRA_FAST)
```

See [API Reference](./API_REFERENCE.md#startup-modes) for details on each mode.

---

## 🔍 Query DSL

Type-safe filtering with queries (requires Kotlin compiler plugin):

```kotlin
data class User(
    @Id val id: String,
    val name: String,
    val age: Int,
    val isActive: Boolean
)

val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// Add data
users.put(User("u001", "Taro", 30, true))
users.put(User("u002", "Hanako", 25, true))
users.put(User("u003", "Jiro", 18, false))

// Query: Users aged 25+ and active
val results = users.runToList { age >= 25 && isActive }
for (user in results) {
    println(user) // User(id=u001, ...), User(id=u002, ...)
}

// Get first result only
val firstUser = users.firstOrNull { age >= 30 }
println(firstUser) // User(id=u001, name=Taro, age=30, isActive=true)

// Check existence
val exists = users.exists { name == "Taro" }
println(exists) // true

// Count
val count = users.count { age < 20 }
println(count) // 1
```

**Supported Operators:**

- Comparison: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Logical: `&&`, `||`, `!`
- Null check: `field == null`, `field != null`
- Collections: `in`, `!in`

---

## 🔄 Upsert (Update or Insert)

Create if doesn't exist, update if exists:

```kotlin
data class Counter(
    @Id val id: String,
    var count: Int = 0
)

val counters = AkkDSL.open<Counter, String>(base, StartupMode.NORMAL)

// Increment counter (create if not exists)
counters.upsert("counter1") {
    count += 1
}

// Run again to update existing record
counters.upsert("counter1") {
    count += 1
}

val counter = counters.get("counter1")
println("Count: ${counter?.count}") // 2
```

**Important:** Using `upsert` requires a **no-argument constructor** on the entity class.

---

## 🛠️ Advanced Configuration (DSL Customization)

For finer control, use DSL builder:

```kotlin
val users = AkkDSL.open<User, String>(base) {
    k = 4                       // Data lanes
    m = 2                       // Parity lanes
    walGroupN = 128             // WAL group commit count
    walGroupMicros = 2000       // WAL group commit time (µs)
    stripeFastMode = true       // Stripe fast mode
    walFastMode = true          // WAL fast mode
    bloomFPRate = 0.01          // Bloom filter false positive rate
    debug = false               // Debug logging
}
```

---

## 🔧 Low-level API Options

For complete control, use `AkkaraDB.Options`:

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.format.api.FlushPolicy
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder

val db = AkkaraDB.open(
    AkkaraDB.Options(
        baseDir = Paths.get("./data/akkdb"),
        k = 4,                                      // Data lanes
        m = 2,                                      // Parity lanes
        flushPolicy = FlushPolicy(
            maxBlocks = 32,
            maxMicros = 500
        ),
        walFastMode = true,                         // WAL fast mode
        stripeFastMode = true,                      // Stripe fast mode
        walGroupN = 64,                             // WAL group commit count
        walGroupMicros = 1_000,                     // WAL group commit time (µs)
        parityCoder = RSParityCoder(2),             // Reed-Solomon parity
        durableCas = false,                         // CAS durability
        useStripeForRead = false,                   // Use Stripe for reads
        bloomFPRate = 0.01,                         // Bloom false positive rate
        debug = false                               // Debug mode
    )
)
```

See [API Reference](./API_REFERENCE.md#options-configuration) for parameter details.

---

## 📌 Best Practices

### 1. Use @Id Annotation

```kotlin
// ✓ Correct: Explicitly mark primary key
data class User(
    @Id val id: String,
    val name: String
)

// ✗ Wrong: Missing @Id annotation
data class User(
    val id: String,  // This alone isn't enough
    val name: String
)
```

### 2. Always Call close()

```kotlin
// ✓ Recommended: Auto-close with use
val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)
users.use {
    it.put(User("u001", "Taro", 30))
    val user = it.get("u001")
}

// Or explicitly close
try {
    val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)
    // ... operations ...
} finally {
    users.close()
}
```

### 3. Manage ByteBufferL Position

For low-level API, don't forget `position(0)`:

```kotlin
// ✓ Correct
val key = ByteBufferL.wrap(bytes).position(0)
db.get(key)

// ✗ Wrong: Undefined position
val key = ByteBufferL.wrap(bytes)
db.get(key) // Unstable behavior
```

### 4. Choose Appropriate Startup Mode

```kotlin
// Production: NORMAL (balanced)
val prod = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// Testing: ULTRA_FAST (fast but low durability)
val test = AkkDSL.open<User, String>(base, StartupMode.ULTRA_FAST)

// Mission-critical: DURABLE (maximum durability)
val critical = AkkDSL.open<User, String>(base, StartupMode.DURABLE)
```

---

## 🎯 Next Steps

After understanding basic usage, learn more from:

- **[API Reference](./API_REFERENCE.md)** - Complete API specification
- **[Architecture](./ARCHITECTURE.md)** - Understand internal design
- **[Benchmarks](./BENCHMARKS.md)** - Performance characteristics

---

## 💡 FAQ

### Q: Can @Id annotation be on multiple fields?

A: No. Each entity requires **exactly one @Id**. Multiple annotations will cause an error.

### Q: Is compiler plugin required for Query DSL?

A: Yes. Using Query DSL (`runToList { }`, `firstOrNull { }`, etc.) requires the Kotlin compiler plugin.
See [Installation](./INSTALLATION.md#compiler-plugin-configuration-required) for details.

### Q: Can I use String or Int as ID?

A: Yes. Any serializable type like `String`, `Int`, `Long`, `UUID`, etc. can be used as ID.

### Q: Can I open multiple PackedTables in the same directory?

A: No. **Only one AkkaraDB instance** can open per database directory. For multiple entity types, use different directories.

---

Next: [API Reference](./API_REFERENCE.md) | [Architecture](./ARCHITECTURE.md) | [Benchmarks](./BENCHMARKS.md)

[Back to Overview](./ABOUT.md)

---