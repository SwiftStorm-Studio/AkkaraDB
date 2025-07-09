package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

/**
 * In-memory write buffer (LSM Level-0).
 *
 *
 * @param thresholdBytes Threshold in bytes for flushing the MemTable to disk.
 * @param onFlush Flush handler. Called when the MemTable is flushed.
 */
class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (List<Record>) -> Unit
) {

    private val map = ConcurrentSkipListMap<ByteBuffer, Record>(FastByteBufferComparator)

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)

    /* ───────── public API ───────── */

    /** Lock-free read (Snapshot semantics) */
    fun get(key: ByteBuffer): Record? = map[key]

    @Synchronized
    fun put(record: Record) {
        insert(record, updateSeq = true)

        if (currentBytes.get() >= thresholdBytes) flush()
    }

    @Synchronized
    fun flush() {
        val entries = map.values.toList()
        if (entries.isEmpty()) return

        map.clear()
        currentBytes.set(0)

        onFlush(entries)
    }

    fun lastSeq(): Long = highestSeqNo.get()

    /* ───────── internal helpers ───────── */

    private fun insert(record: Record, updateSeq: Boolean) {
        val keyCopy = record.key.slice().asReadOnlyBuffer()
        val prev = map.put(keyCopy, record)

        currentBytes.addAndGet(sizeOf(record) - (prev?.let(::sizeOf) ?: 0L))

        if (updateSeq) highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
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
