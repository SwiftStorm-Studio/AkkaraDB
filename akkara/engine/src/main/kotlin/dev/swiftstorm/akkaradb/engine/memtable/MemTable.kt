package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.nio.ByteOrder
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
 * A thread-safe in-memory table for storing and managing `Record` objects.
 * Supports LRU eviction, flushing, and iteration over records.
 *
 * @property thresholdBytes The maximum size in bytes before triggering a flush.
 * @property onFlush Callback invoked when the table is flushed.
 * @property onEvict Optional callback invoked when records are evicted.
 * @property maxEntries Optional maximum number of entries allowed in the table.
 * @property executor Executor service used for asynchronous flushing.
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
    private fun newLruMap(): LinkedHashMap<ByteBuffer, Record> =
        object : LinkedHashMap<ByteBuffer, Record>(16, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<ByteBuffer, Record>): Boolean {
                if (maxEntries != null && size > maxEntries) {
                    currentBytes.addAndGet(-this@MemTable.sizeOf(eldest.value))
                    onEvict?.invoke(listOf(eldest.value))
                    return true
                }
                return false
            }
        }

    private val mapRef = AtomicReference<LinkedHashMap<ByteBuffer, Record>>(newLruMap())

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)

    private val flushPending = AtomicBoolean(false)

    private val lock = ReentrantReadWriteLock()

    /**
     * Retrieves a record by its key.
     *
     * @param key The key of the record to retrieve.
     * @return The record associated with the key, or `null` if not found.
     */
    fun get(key: ByteBuffer): Record? = lock.read { mapRef.get()[key] }

    /**
     * Retrieves all records in the table.
     *
     * @return A list of all records.
     */
    fun getAll(): List<Record> = lock.read { mapRef.get().values.toList() }

    /**
     * Inserts or updates a record in the table.
     * Triggers a flush if the size exceeds the threshold.
     *
     * @param record The record to insert or update.
     */
    fun put(record: Record): Boolean {
        var shouldFlush = false
        val accepted = lock.write {
            val keyCopy = record.key.duplicate().apply { rewind() }.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)
            val valCopy = record.value.duplicate().apply { rewind() }.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)
            val map = mapRef.get()
            val prev = map[keyCopy]
            if (prev != null && record.seqNo < prev.seqNo) return@write false

            map[keyCopy] = Record(keyCopy, valCopy, record.seqNo, flags = if (record.isTombstone) Record.FLAG_TOMBSTONE else 0)

            if (record.seqNo > highestSeqNo.get()) {
                highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
            }
            val delta = sizeOf(map[keyCopy]!!) - (prev?.let { sizeOf(it) } ?: 0L)
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
        if (flushPending.compareAndSet(false, true) || currentBytes.get() > 0) {
            flushInternal()
        }
    }

    /**
     * Evicts the oldest `n` records from the table.
     *
     * @param n The number of records to evict (default is 1).
     */
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

    private fun flushInternal() {
        var toFlush: List<Record>? = null
        var bytesFlushed = 0L

        lock.write {
            val oldMap = mapRef.getAndSet(newLruMap())
            if (oldMap.isNotEmpty()) {
                toFlush = ArrayList(oldMap.values)
                bytesFlushed = oldMap.values.sumOf { sizeOf(it) }
            } else {
                flushPending.set(false)
                return
            }
        }
        currentBytes.addAndGet(-bytesFlushed)

        try {
            onFlush.invoke(toFlush!!)
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            flushPending.set(false)
        }
    }

    fun iterator(from: ByteBuffer? = null, toExclusive: ByteBuffer? = null): Iterator<Record> {
        val snapshot: List<Record> = lock.read { mapRef.get().values.toList() }

        val filteredSorted = snapshot.asSequence()
            .filter { r ->
                val k = r.key.duplicate().apply { rewind() }
                val geFrom = from?.let { cmp(k, it) >= 0 } ?: true
                val ltTo = toExclusive?.let { cmp(k, it) < 0 } ?: true
                geFrom && ltTo
            }
            .sortedWith { a, b -> cmp(a.key, b.key) }
            .toList()

        return filteredSorted.iterator()
    }

    private fun sizeOf(r: Record): Long =
        (r.key.remaining() + r.value.remaining() + Long.SIZE_BYTES).toLong()

    private fun cmp(a: ByteBuffer, b: ByteBuffer): Int {
        val aa = a.duplicate().apply { rewind() }
        val bb = b.duplicate().apply { rewind() }
        while (aa.hasRemaining() && bb.hasRemaining()) {
            val x = aa.get().toInt() and 0xFF
            val y = bb.get().toInt() and 0xFF
            if (x != y) return x - y
        }
        return aa.remaining() - bb.remaining()
    }
}
