package dev.swiftstorm.akkaradb.engine.memtable

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

class MemTable(
    private val thresholdBytes: Long,
    private val onFlush: (List<Record>) -> Unit
) {

    private val map =
        ConcurrentSkipListMap<ByteBuffer, Record>(FastByteBufferComparator)

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)

    /* ---------- public API ---------- */

    fun get(key: ByteBuffer): Record? = map[key]

    @Synchronized
    fun put(record: Record) {
        insert(record, updateSeq = true)
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

    /* ---------- internal helpers ---------- */

    private fun insert(record: Record, updateSeq: Boolean) {
        val key = record.key.slice().asReadOnlyBuffer()
        val prev = map.put(key, record)

        currentBytes.addAndGet(sizeOf(record) - (prev?.let(::sizeOf) ?: 0))

        if (updateSeq) {
            highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
        }

        if (currentBytes.get() >= thresholdBytes) flush()
    }

    private fun sizeOf(record: Record): Long {
        val key = record.key
        val value = record.value
        return (key.remaining() + value.remaining() + Long.SIZE_BYTES).toLong()
    }
}

/* helpers */
object FastByteBufferComparator : Comparator<ByteBuffer> {
    override fun compare(a: ByteBuffer, b: ByteBuffer): Int {
        val apos = a.position()
        val bpos = b.position()
        val aRem = a.remaining()
        val bRem = b.remaining()

        val mismatch = a.mismatch(b)
        if (mismatch != -1) {
            val ba = a.get(apos + mismatch).toInt() and 0xFF
            val bb = b.get(bpos + mismatch).toInt() and 0xFF
            return ba - bb
        }

        return aRem - bRem
    }
}
