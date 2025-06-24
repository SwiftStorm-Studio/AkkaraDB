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
        ConcurrentSkipListMap<ByteBuffer, Record>(ByteBufferLexicographicComparator)

    private val currentBytes = AtomicLong(0)
    private val highestSeqNo = AtomicLong(0)

    /* ---------- public API ---------- */

    fun put(record: Record) = insert(record, updateSeq = true)

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
        val prev = map.put(record.key, record)

        currentBytes.addAndGet(sizeOf(record) - (prev?.let(::sizeOf) ?: 0))

        if (updateSeq) {
            highestSeqNo.updateAndGet { old -> max(old, record.seqNo) }
        }

        if (currentBytes.get() >= thresholdBytes) flush()
    }

    private fun sizeOf(record: Record): Long =
        (record.key.remaining() + record.value.remaining() + java.lang.Long.BYTES).toLong()
}

/* helpers */
object ByteBufferLexicographicComparator : Comparator<ByteBuffer> {
    override fun compare(a: ByteBuffer, b: ByteBuffer): Int {
        val minLen = minOf(a.remaining(), b.remaining())
        for (i in 0 until minLen) {
            val diff = (a.get(a.position() + i).toInt() and 0xFF) -
                    (b.get(b.position() + i).toInt() and 0xFF)
            if (diff != 0) return diff
        }
        return a.remaining() - b.remaining()
    }
}