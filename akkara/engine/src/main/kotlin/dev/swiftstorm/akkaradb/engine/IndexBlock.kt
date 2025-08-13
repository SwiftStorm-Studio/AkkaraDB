package dev.swiftstorm.akkaradb.engine

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * Flat index: (firstKey, data-block offset) * N
 *
 * Binary layout (little-endian):
 *   repeat N times {
 *     [keyLen: u16][key bytes][offset: s64]
 *   }
 *
 * Notes
 *  - `firstKey` is stored as raw bytes (position = 0 on add())
 *  - `offset` is the absolute file offset of the data block
 */
class IndexBlock {
    private val keys = ArrayList<ByteBuffer>()
    private val offsets = ArrayList<Long>()

    fun add(firstKey: ByteBuffer, offset: Long) {
        val ro = firstKey.asReadOnlyBuffer().apply { rewind() }
        require(ro.remaining() <= 0xFFFF) { "key too long for u16: ${ro.remaining()} bytes" }
        keys += ro
        offsets += offset
    }

    /** Returns offset of the last block whose firstKey ≤ key, or -1 if none. */
    fun lookup(key: ByteBuffer): Long {
        var lo = 0
        var hi = keys.lastIndex
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val cmp = keys[mid].compareTo(key)
            if (cmp <= 0) lo = mid + 1 else hi = mid - 1
        }
        return if (hi >= 0) offsets[hi] else -1
    }

    /* ───────── (de)serialization ───────── */

    fun writeTo(ch: WritableByteChannel) {
        val capacity = keys.sumOf { 2 /*u16*/ + it.remaining() + Long.SIZE_BYTES }
        val buf = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN)
        keys.zip(offsets).forEach { (k, off) ->
            buf.putShort(k.remaining().toShort())
            buf.put(k.duplicate())
            buf.putLong(off)
        }
        buf.flip(); ch.write(buf)
    }

    companion object {
        fun readFrom(ch: ReadableByteChannel, size: Int): IndexBlock =
            ByteBuffer.allocate(size).also { ch.read(it); it.flip() }
                .let { readFrom(it, size) }

        fun readFrom(buf: ByteBuffer, size: Int): IndexBlock {
            val ib = IndexBlock()
            buf.order(ByteOrder.LITTLE_ENDIAN)
            val end = buf.position() + size
            while (buf.position() < end) {
                require(end - buf.position() >= 2) { "truncated keyLen" }
                val kLen = buf.short.toInt() and 0xFFFF
                require(kLen >= 0 && end - buf.position() >= kLen + 8) { "invalid kLen=$kLen or truncated entry" }

                val keyArr = ByteArray(kLen)
                buf.get(keyArr)
                val key = ByteBuffer.wrap(keyArr) // pos=0, limit=kLen

                val off = buf.long
                ib.add(key, off)
            }
            return ib
        }
    }
}
