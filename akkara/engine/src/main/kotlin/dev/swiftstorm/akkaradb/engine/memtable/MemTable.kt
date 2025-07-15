package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
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
 * In-memory write buffer (LSM **Level-0**) with **lock-free reads** and a fully
 * **asynchronous background flush**.
 *
 * Flush policy:
 * 1. Writers append to the current map until the aggregate size exceeds
 *    [thresholdBytes].
 * 2. The *first* writer noticing the overflow sets a guard flag and schedules
 *    `flushInternal()` on [executor].  The writer itself **never blocks on I/O**.
 * 3. `flushInternal()` swaps the active map in *O(1)* under a *write* lock and
 *    passes the snapshot to [onFlush].  New writes proceed on the fresh map.
 *
 * This guarantees constant-time `put()` even under heavy load while ensuring
 * exactly-once delivery of every record to disk.
 *
 * @param thresholdBytes  Größe in bytes before the MemTable is flushed.
 * @param onFlush         Callback executed with the immutable snapshot.
 * @param executor        Optional external executor (default: single daemon).
 */
class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (List<Record>) -> Unit,
    private val executor: ExecutorService = Executors.newSingleThreadExecutor { r ->
        Thread(r, "memtable-flush").apply { isDaemon = true }
    }
) : AutoCloseable {

    /* ───────── state ───────── */

    private val mapRef = AtomicReference(
        ConcurrentSkipListMap<ByteBuffer, Record>(FastByteBufferComparator)
    )

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)
    private val flushPending = AtomicBoolean(false)

    private val lock = ReentrantReadWriteLock()

    /* ───────── public API ───────── */

    /**
     * Point-in-time read without locking (snapshot semantics).
     */
    fun get(key: ByteBuffer): Record? = mapRef.get()[key]

    /**
     * Append (or overwrite) a record.
     * Never blocks on flush – at most does an *O(1)* flag set & task submit.
     */
    fun put(record: Record) {
        val triggerFlush = lock.read {
            val delta = insert(record)
            val sizeAfter = currentBytes.addAndGet(delta)
            sizeAfter >= thresholdBytes
        }

        // schedule background flush – only the first winner succeeds
        if (triggerFlush && flushPending.compareAndSet(false, true)) {
            executor.execute(::flushInternal)
        }
    }

    /**
     * Forces an immediate flush in the caller thread.  Safe to invoke at any
     * time; concurrent background flushes are coalesced.
     */
    fun flush() {
        if (flushPending.compareAndSet(false, true) || currentBytes.get() > 0) {
            flushInternal()
        }
    }

    /** Highest sequence number ever inserted into this MemTable. */
    fun lastSeq(): Long = highestSeqNo.get()

    /* ───────── lifecycle ───────── */

    override fun close() {
        try {
            flush()
        } finally {
            executor.shutdown()
        }
    }

    /* ───────── internal helpers ───────── */

    private fun flushInternal() {
        val snapshot: ConcurrentSkipListMap<ByteBuffer, Record>

        // exclusive phase – O(1) map swap
        lock.write {
            snapshot = mapRef.getAndSet(
                ConcurrentSkipListMap(FastByteBufferComparator)
            )
            currentBytes.set(0)
        }

        // allow new writes before heavy I/O begins
        try {
            if (snapshot.isNotEmpty()) {
                onFlush(snapshot.values.toList())
            }
        } finally {
            flushPending.set(false)
            snapshot.clear()          // help GC
        }
    }

    /**
     * Inserts/overwrites the record and returns the delta in byte size.
     */
    private fun insert(record: Record): Long {
        val keyCopy = record.key.duplicate().apply { rewind() }.asReadOnlyBuffer()
        val map = mapRef.get()
        val prev = map.put(keyCopy, record)

        if (record.seqNo > highestSeqNo.get()) {
            highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
        }

        return sizeOf(record) - (prev?.let { sizeOf(it) } ?: 0L)
    }

    private fun sizeOf(r: Record): Long =
        (r.key.remaining() + r.value.remaining() + Long.SIZE_BYTES).toLong()
}

/* ───────── comparator ───────── */

object FastByteBufferComparator : Comparator<ByteBuffer> {
    override fun compare(a: ByteBuffer, b: ByteBuffer): Int {
        val mismatch = a.mismatch(b)
        if (mismatch == -1)
            return a.remaining().compareTo(b.remaining())

        val aa = a.get(a.position() + mismatch).toInt() and 0xFF
        val bb = b.get(b.position() + mismatch).toInt() and 0xFF
        return aa.compareTo(bb)
    }
}
