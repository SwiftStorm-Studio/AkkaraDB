package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.math.max

/* * MemTable is an in-memory key-value store that uses a LinkedHashMap to maintain
 * LRU order of entries. It supports automatic flushing to disk when a size threshold
 * is reached, and can evict the oldest entries if a maximum entry count is set.
 *
 * @param thresholdBytes The size in bytes at which the MemTable will trigger a flush.
 * @param onFlush Callback invoked with the records to flush when the threshold is reached.
 * @param onEvict Optional callback invoked with evicted records when eviction occurs.
 * @param maxEntries Optional maximum number of entries before eviction is triggered.
 */
class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (List<Record>) -> Unit,
    private val onEvict: ((List<Record>) -> Unit)? = null,
    private val maxEntries: Int? = null,
    private val executor: ExecutorService = Executors.newSingleThreadExecutor { r ->
        Thread(r, "memtable-flush").apply { isDaemon = true }
    }
) : AutoCloseable {
    /* ---- LRU-Ordered map ---- */
    private val lruMap = object : LinkedHashMap<ByteBuffer, Record>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<ByteBuffer, Record>): Boolean {
            if (maxEntries != null && size > maxEntries) {
                onEvict?.invoke(listOf(eldest.value))
                return true
            }
            return false
        }
    }
    private val mapRef = AtomicReference(lruMap)

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)
    private val flushPending = AtomicBoolean(false)
    private val lock = ReentrantReadWriteLock()

    fun get(key: ByteBuffer): Record? = lock.read { mapRef.get()[key] }

    fun put(record: Record) {
        val triggerFlush = lock.write {
            val keyCopy = record.key.duplicate().apply { rewind() }.asReadOnlyBuffer()
            val map = mapRef.get()
            val prev = map.put(keyCopy, record)
            if (record.seqNo > highestSeqNo.get()) {
                highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
            }
            val delta = sizeOf(record) - (prev?.let { sizeOf(it) } ?: 0L)
            val sizeAfter = currentBytes.addAndGet(delta)
            sizeAfter >= thresholdBytes
        }
        if (triggerFlush && flushPending.compareAndSet(false, true)) {
            executor.execute(::flushInternal)
        }
    }

    fun flush() {
        if (flushPending.compareAndSet(false, true) || currentBytes.get() > 0) {
            flushInternal()
        }
    }

    fun evictOldest(n: Int = 1) {
        lock.write {
            val map = mapRef.get()
            val evicted = mutableListOf<Record>()
            repeat(n) {
                val eldest = map.entries.iterator().takeIf { it.hasNext() }?.next()
                if (eldest != null) {
                    evicted += eldest.value
                    map.remove(eldest.key)
                }
            }
            if (evicted.isNotEmpty()) onEvict?.invoke(evicted)
        }
    }

    /**
     * Returns the highest sequence number currently in the MemTable.
     * This is useful for tracking the latest record added.
     */
    fun lastSeq(): Long = highestSeqNo.get()

    /**
     * Returns the next sequence number to be used for a new record.
     * This is typically used when inserting a new record to ensure
     * it has a unique sequence number.
     */
    fun nextSeq(): Long = highestSeqNo.updateAndGet { it + 1 }

    override fun close() {
        try {
            flush()
        } finally {
            executor.shutdown()
        }
    }

    private fun flushInternal() {
        val snapshot: List<Record>
        lock.read { snapshot = mapRef.get().values.toList() }
        try {
            if (snapshot.isNotEmpty()) {
                onFlush(snapshot)
            }
        } finally {
            flushPending.set(false)
        }
    }

    private fun sizeOf(r: Record): Long = (r.key.remaining() + r.value.remaining() + Long.SIZE_BYTES).toLong()
}
