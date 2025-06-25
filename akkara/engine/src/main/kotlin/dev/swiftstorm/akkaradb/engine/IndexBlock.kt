package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

class IndexBlock {
    private val keys = ArrayList<ByteBuffer>()
    private val offsets = ArrayList<Long>()

    fun add(firstKey: ByteBuffer, offset: Long) {
        keys += firstKey.asReadOnlyBuffer()
        offsets += offset
    }

    fun lookup(key: ByteBuffer): Long {
        var lo = 0;
        var hi = keys.lastIndex
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val cmp = keys[mid].compareTo(key)
            if (cmp <= 0) lo = mid + 1 else hi = mid - 1
        }
        return if (hi >= 0) offsets[hi] else -1
    }

    /* ---- (de)serialization ---- */

    fun writeTo(ch: WritableByteChannel) {
        val buf = ByteBuffer.allocate(keys.sumOf { it.remaining() + 10 }) // rough
        keys.zip(offsets).forEach { (k, off) ->
            VarIntCodec.writeInt(buf, k.remaining())
            buf.put(k.duplicate())
            buf.putLong(off)
        }
        buf.flip(); ch.write(buf)
    }

    companion object {
        fun readFrom(ch: ReadableByteChannel, size: Int): IndexBlock {
            val buf = ByteBuffer.allocate(size)
            ch.read(buf); buf.flip()
            val ib = IndexBlock()
            while (buf.hasRemaining()) {
                val len = VarIntCodec.readInt(buf)
                val key = ByteBuffer.allocate(len)
                buf.get(key.array())
                val off = buf.long
                ib.add(key, off)
            }
            return ib
        }
    }
}