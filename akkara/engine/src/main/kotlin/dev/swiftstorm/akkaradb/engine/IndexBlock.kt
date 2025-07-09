package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

/**
 * Flat index: (firstKey, data-block offset) * N
 *
 * * firstKey = raw key bytes (position = 0)
 * * offset   = absolute file offset of the data-block
 */
class IndexBlock {
    private val keys = ArrayList<ByteBuffer>()
    private val offsets = ArrayList<Long>()

    fun add(firstKey: ByteBuffer, offset: Long) {
        keys += firstKey.asReadOnlyBuffer().apply { rewind() }
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
        val capacity = keys.sumOf { VarIntCodec.encodedSize(it.remaining()) + it.remaining() + Long.SIZE_BYTES }
        val buf = ByteBuffer.allocate(capacity)
        keys.zip(offsets).forEach { (k, off) ->
            VarIntCodec.writeInt(buf, k.remaining())
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
            val end = buf.position() + size
            while (buf.position() < end) {
                val len = VarIntCodec.readInt(buf)
                val keyArr = ByteArray(len)
                buf.get(keyArr)
                val key = ByteBuffer.wrap(keyArr)               // pos=0, limit=len
                val off = buf.long
                ib.add(key, off)
            }
            return ib
        }
    }
}
