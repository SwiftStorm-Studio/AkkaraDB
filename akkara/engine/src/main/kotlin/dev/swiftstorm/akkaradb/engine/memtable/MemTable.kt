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

    private fun sizeOf(record: Record): Long {
        val keyLen = record.key.remaining()
        val valLen = record.value.remaining()
        return (keyLen + valLen + Long.SIZE_BYTES).toLong()
    }
}

/* helpers */
object FastByteBufferComparator : Comparator<ByteBuffer> {

    private val hasMismatch = runCatching {
        ByteBuffer::class.java.getMethod("mismatch", ByteBuffer::class.java)
    }.isSuccess

    override fun compare(a: ByteBuffer, b: ByteBuffer): Int {
        val aRem = a.remaining()
        val bRem = b.remaining()

        if (hasMismatch) {
            val pos: Int = a.mismatch(b)
            if (pos != -1) {
                val ba = a.get(a.position() + pos).toInt() and 0xFF
                val bb = b.get(b.position() + pos).toInt() and 0xFF
                return ba - bb
            }
            return aRem - bRem
        }

        var i = 0
        val minLongs = minOf(aRem, bRem) ushr 3
        while (i < minLongs) {
            val diff = a.getLong(a.position() + (i shl 3)) xor
                    b.getLong(b.position() + (i shl 3))
            if (diff != 0L) {
                val shift = java.lang.Long.numberOfLeadingZeros(diff) xor 56
                val byteA = ((diff ushr shift) and 0xFF).toInt()
                val byteB = ((diff ushr shift) and 0xFF).toInt()
                return byteA - byteB
            }
            i++
        }
        var idx = i shl 3
        while (idx < minOf(aRem, bRem)) {
            val diff = (a.get(a.position() + idx).toInt() and 0xFF) -
                    (b.get(b.position() + idx).toInt() and 0xFF)
            if (diff != 0) return diff
            idx++
        }
        return aRem - bRem
    }
}