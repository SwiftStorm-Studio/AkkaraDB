package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.MemRecord
import dev.swiftstorm.akkaradb.common.binpack.AdapterResolver
import dev.swiftstorm.akkaradb.common.binpack.BinPackBufferPool
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.format.api.FlushPolicy
import java.io.Closeable
import java.nio.file.Path
import kotlin.reflect.typeOf

/**
 * AkkaraDSL (v3)
 *
 * - Users interact with the DB via this DSL, not the low-level engine.
 * - Keys and values are arbitrary Kotlin objects that are encoded/decoded using
 *   the common BinPack (ByteBufferL-only) module.
 * - On the engine side we use AkkaraDB (v3) with WAL-before-ack semantics.
 *
 * Notes:
 * - "MemRecord" in this codebase is the in-memory equivalent of the previous
 *   generation's "Record" type. It is the hot-path KV holder for the MemTable
 *   and flush pipeline. The on-disk view is represented by format-api's
 *   RecordView (zero-copy slices unpacked from 32 KiB blocks).
 * - The DSL uses BinPack from the common module to serialize application types
 *   into ByteBufferL for storage and to deserialize them on reads.
 */
object AkkaraDSL {

    // ---------------- Options ----------------

    data class Options(
        var baseDir: Path,
        var k: Int = 4,
        var m: Int = 2,
        var fastMode: Boolean = true,
        var walGroupN: Int = 32,
        var walGroupMicros: Long = 500,
        var flushMaxBlocks: Int = 32,
        var flushMaxMicros: Long = 500,
    )

    class OptionsBuilder(private val baseDir: Path) {
        var k: Int = 4
        var m: Int = 2
        var fastMode: Boolean = true
        var walGroupN: Int = 32
        var walGroupMicros: Long = 500
        var flushMaxBlocks: Int = 32
        var flushMaxMicros: Long = 500

        fun build(): Options = Options(
            baseDir = baseDir,
            k = k,
            m = m,
            fastMode = fastMode,
            walGroupN = walGroupN,
            walGroupMicros = walGroupMicros,
            flushMaxBlocks = flushMaxBlocks,
            flushMaxMicros = flushMaxMicros,
        )
    }

    // ---------------- Entry points ----------------

    inline fun <reified K : Any, reified V : Any> open(
        baseDir: Path,
        noinline configure: OptionsBuilder.() -> Unit = {}
    ): Table<K, V> {
        val opts = OptionsBuilder(baseDir).apply(configure).build()
        val engine = AkkaraDB.open(
            AkkaraDB.Options(
                baseDir = opts.baseDir,
                k = opts.k,
                m = opts.m,
                flushPolicy = FlushPolicy(maxBlocks = opts.flushMaxBlocks, maxMicros = opts.flushMaxMicros),
                fastMode = opts.fastMode,
                walGroupN = opts.walGroupN,
                walGroupMicros = opts.walGroupMicros,
                parityCoder = null
            )
        )
        // Capture adapters for key and value types so Table methods don't need reified params
        val kAdapter: TypeAdapter<K> = AdapterResolver.getAdapterForType(typeOf<K>()) as TypeAdapter<K>
        val vAdapter: TypeAdapter<V> = AdapterResolver.getAdapterForType(typeOf<V>()) as TypeAdapter<V>
        return Table(engine, kAdapter, vAdapter)
    }

    /**
     * Typed KV facade backed by AkkaraDB.
     * - Keys [K] and values [V] are encoded/decoded with ByteBufferL TypeAdapters.
     * - The underlying DB stores ByteBufferL key/value pairs.
     */
    class Table<K : Any, V : Any>(private val db: AkkaraDB, private val kAdapter: TypeAdapter<K>, private val vAdapter: TypeAdapter<V>) : Closeable {
        override fun close() = db.close()

        // ---------- CRUD ----------
        fun put(key: K, value: V): Long {
            val k = encode(kAdapter, key)
            val v = encode(vAdapter, value)
            return db.put(k, v)
        }

        fun get(key: K): V? {
            val k = encode(kAdapter, key)
            val bytes = db.get(k) ?: return null
            return decode(vAdapter, bytes)
        }

        fun delete(key: K): Long {
            val k = encode(kAdapter, key)
            return db.delete(k)
        }

        fun compareAndSwap(key: K, expectedSeq: Long, newValue: V?): Boolean {
            val k = encode(kAdapter, key)
            val v = newValue?.let { encode(vAdapter, it) }
            return db.compareAndSwap(k, expectedSeq, v)
        }

        /**
         * Iterate all in-memory records (snapshot) as decoded entries.
         * For full DB iteration you may also merge SSTables on the application side.
         */
        fun iterator(range: MemTable.KeyRange = MemTable.KeyRange.ALL): Sequence<Pair<K, V?>> = sequence {
            for (mr: MemRecord in db.iterator(range)) {
                val k: K = decode(kAdapter, mr.key)
                val v: V? = if (mr.tombstone) null else decode(vAdapter, mr.value)
                yield(k to v)
            }
        }

        fun flush() = db.flush()
        fun lastSeq(): Long = db.lastSeq()
    }
}

// ---------------- small helpers ----------------

private fun <T : Any> encode(adapter: TypeAdapter<T>, value: T): ByteBufferL {
    val size = adapter.estimateSize(value).coerceAtLeast(32)
    val buf = BinPackBufferPool.get(size)
    val bb = buf
    adapter.write(value, bb)
    // Switch to read-mode for external APIs: duplicate as read-only with position=0
    val out = bb.asReadOnlyDuplicate()
    out.position(0)
    return out
}

private fun <T : Any> decode(adapter: TypeAdapter<T>, bytes: ByteBufferL): T {
    val ro = bytes.asReadOnlyDuplicate().position(0)
    return adapter.read(ro)
}
