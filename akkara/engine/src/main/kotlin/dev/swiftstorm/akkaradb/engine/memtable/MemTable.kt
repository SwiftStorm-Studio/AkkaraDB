package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.compareTo
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

/**
 * Thread-safe in-memory table with LRU eviction, flush, and range iteration.
 * - Key equality is by **content** (unsigned-lex compare for ordering only).
 * - Stored buffers are canonicalized to RO views (pos=0..limit).
 */
class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (MutableList<Record>) -> Unit,
    private val onEvict: ((List<Record>) -> Unit)? = null,
    private val maxEntries: Int? = null,
    private val executor: ExecutorService = Executors.newSingleThreadExecutor { r ->
        Thread(r, "memtable-flush").apply { isDaemon = true }
    }
) : AutoCloseable {

    /* ---- LRU-Ordered map (content-based key) ---- */
    private fun newLruMap(): LinkedHashMap<Key, Record> =
        object : LinkedHashMap<Key, Record>(16, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, Record>): Boolean {
                if (maxEntries != null && size > maxEntries) {
                    currentBytes.addAndGet(-this@MemTable.sizeOf(eldest.value))
                    onEvict?.invoke(listOf(eldest.value))
                    return true
                }
                return false
            }
        }

    private val mapRef = AtomicReference<LinkedHashMap<Key, Record>>(newLruMap())

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)
    private val flushPending = AtomicBoolean(false)
    private val lock = ReentrantReadWriteLock()

    /** O(1) lookup by content-equal key. */
    fun get(key: ByteBufferL): Record? = lock.read {
        mapRef.get()[wrapKey(key)]
    }

    /** Snapshot of all records (copy). */
    fun getAll(): List<Record> = lock.read { mapRef.get().values.toList() }

    /**
     * Insert or update. If an existing key exists and its seqNo is newer, reject.
     * Triggers async flush when size exceeds threshold.
     */
    fun put(record: Record): Boolean {
        var shouldFlush = false
        val accepted = lock.write {
            // Canonicalize buffers (RO, pos=0) for storage
            val keyRO = record.key.duplicate().apply { rewind() }.asReadOnly()
            val valRO = record.value.duplicate().apply { rewind() }.asReadOnly()
            val k = wrapKey(keyRO)

            val map = mapRef.get()
            val prev = map[k]
            if (prev != null && record.seqNo < prev.seqNo) return@write false

            val stored = Record(
                keyRO, valRO, record.seqNo,
                flags = if (record.isTombstone) Record.FLAG_TOMBSTONE else 0
            )

            map[k] = stored

            if (record.seqNo > highestSeqNo.get()) {
                highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
            }
            val delta = sizeOf(stored) - (prev?.let { sizeOf(it) } ?: 0L)
            val sizeAfter = currentBytes.addAndGet(delta)
            shouldFlush = sizeAfter >= thresholdBytes
            true
        }
        if (shouldFlush && flushPending.compareAndSet(false, true)) {
            executor.execute(::flushInternal)
        }
        return accepted
    }

    fun flush() {
        if (!flushPending.compareAndSet(false, true)) return
        try {
            flushInternal()
        } finally {
            flushPending.set(false)
        }
    }

    /** Evict oldest n (access-order) entries. */
    fun evictOldest(n: Int = 1) {
        lock.write {
            val map = mapRef.get()
            val it = map.entries.iterator()
            val evicted = ArrayList<Record>(n)
            var bytesFreed = 0L

            var i = 0
            while (i < n && it.hasNext()) {
                val e = it.next()
                evicted += e.value
                bytesFreed += sizeOf(e.value)
                it.remove()
                i++
            }

            if (bytesFreed > 0) currentBytes.addAndGet(-bytesFreed)
            if (evicted.isNotEmpty()) onEvict?.invoke(evicted)
        }
    }

    fun lastSeq(): Long = highestSeqNo.get()
    fun nextSeq(): Long = highestSeqNo.updateAndGet { it + 1 }

    override fun close() {
        try {
            flush()
        } finally {
            executor.shutdown()
        }
    }

    /* ---- flush internals ---- */
    private fun flushInternal() {
        val toFlush: MutableList<Record> = lock.write {
            val old = mapRef.getAndSet(newLruMap())
            if (old.isEmpty()) return
            ArrayList(old.values)
        }

        val bytesFlushed = toFlush.sumOf { sizeOf(it) }
        currentBytes.addAndGet(-bytesFlushed)

        try {
            onFlush.invoke(toFlush)
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    /* ---- iteration with range filter (unsigned lex) ---- */
    fun iterator(from: ByteBufferL? = null, toExclusive: ByteBufferL? = null): Iterator<Record> {
        val snapshot: List<Record> = lock.read { mapRef.get().values.toList() }

        val filteredSorted = snapshot.asSequence()
            .filter { r ->
                val k = r.key.duplicate().apply { rewind() }
                val geFrom = from?.let { k >= it } ?: true
                val ltTo = toExclusive?.let { k < it } ?: true
                geFrom && ltTo
            }
            .sortedWith { a, b -> a.key.compareTo(b.key) } // unsigned lex
            .toList()

        return filteredSorted.iterator()
    }

    /* ---- helpers ---- */

    private fun sizeOf(r: Record): Long =
        (r.key.remaining + r.value.remaining + Long.SIZE_BYTES).toLong()

    /** Canonical RO view (pos=0..limit). */
    private fun canonicalRO(b: ByteBufferL): ByteBufferL =
        b.duplicate().apply { rewind() }.asReadOnly()

    /** Content hash compatible with RO canonical view. */
    private fun contentHash(b: ByteBufferL): Int =
        b.asReadOnlyByteBuffer().duplicate().apply { rewind() }.hashCode()

    /** Content-equal comparison without consuming positions. */
    private fun contentEquals(a: ByteBufferL, b: ByteBufferL): Boolean {
        val aa = a.asReadOnlyByteBuffer().slice()
        val bb = b.asReadOnlyByteBuffer().slice()
        if (aa.remaining() != bb.remaining()) return false
        val mm = aa.mismatch(bb)
        return mm == -1
    }

    /** Wrap a key buffer into a content-hashed key object (canonicalized). */
    private fun wrapKey(key: ByteBufferL): Key {
        val ro = canonicalRO(key)
        return Key(ro, contentHash(ro))
    }

    /** Content-based key for the LRU map (hash precomputed). */
    private data class Key(val ro: ByteBufferL, val hash: Int) {
        override fun hashCode(): Int = hash
        override fun equals(other: Any?): Boolean =
            other is Key && contentEquals(ro, other.ro)

        // static helper to avoid capturing outer
        private fun contentEquals(a: ByteBufferL, b: ByteBufferL): Boolean {
            val aa = a.asReadOnlyByteBuffer().slice()
            val bb = b.asReadOnlyByteBuffer().slice()
            if (aa.remaining() != bb.remaining()) return false
            val mm = aa.mismatch(bb)
            return mm == -1
        }
    }
}
