@file:Suppress("DuplicatedCode", "unused")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.compareTo
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.WritableByteChannel
import kotlin.math.min

/**
 * Flat outer index with **fixed-length 32-byte key prefix** per entry.
 *
 * Binary layout (little-endian):
 *   repeat N times {
 *     [firstKeyPrefix: 32 bytes][blockOffset: s64]
 *   }
 *
 * Notes
 *  - The key is the first 32 bytes of the block's first record key. If the
 *    actual key is shorter than 32 bytes, it is right-padded with zeros.
 *  - Offsets are absolute file offsets to the beginning of the on-disk block
 *    header (i.e., the [len:u32] field).
 */
class IndexBlock {
    private val keys = ArrayList<ByteBufferL>() // each 32 bytes
    private val offsets = ArrayList<Long>()

    fun add(firstKey: ByteBuffer, offset: Long) {
        val dup = firstKey.duplicate().apply { rewind() }
        val bb = ByteBufferL.allocate(KEY_BYTES)
        val n = min(KEY_BYTES, dup.remaining())
        bb.put(dup.slice().limit(n)) // copy n bytes
        while (bb.position < KEY_BYTES) bb.put(0) // pad zeros
        bb.flip()
        keys += bb.asReadOnly()
        offsets += offset
    }

    fun lookup(key: ByteBuffer): Long {
        val probe = prefix32(key)
        var lo = 0
        var hi = keys.lastIndex
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val cmp = keys[mid].compareTo(probe) // unsigned lex
            if (cmp <= 0) lo = mid + 1 else hi = mid - 1
        }
        return if (hi >= 0) offsets[hi] else -1
    }

    fun writeTo(ch: WritableByteChannel) {
        val cap = keys.size * ENTRY_BYTES
        val buf = ByteBuffer.allocate(cap).order(ByteOrder.LITTLE_ENDIAN)
        for (i in keys.indices) {
            buf.put(keys[i].asReadOnlyByteBuffer())
            buf.putLong(offsets[i])
        }
        buf.flip(); ch.write(buf)
    }

    companion object {
        private const val KEY_BYTES = 32
        private const val ENTRY_BYTES = KEY_BYTES + java.lang.Long.BYTES

        fun readFrom(buf: ByteBuffer, size: Int): IndexBlock {
            require(size % ENTRY_BYTES == 0)
            val ib = IndexBlock()
            buf.order(ByteOrder.LITTLE_ENDIAN)
            val count = size / ENTRY_BYTES
            repeat(count) {
                val bb = ByteBufferL.allocate(KEY_BYTES)
                val dst = bb.toMutableByteBuffer()

                require(buf.remaining() >= KEY_BYTES) { "index block truncated" }
                val end = buf.position() + KEY_BYTES
                val prevLimit = buf.limit()
                buf.limit(end)
                dst.put(buf)
                buf.limit(prevLimit)

                bb.flip()
                ib.keys += bb.asReadOnly()
                ib.offsets += buf.long
            }

            return ib
        }

        private fun prefix32(key: ByteBuffer): ByteBufferL {
            val dup = key.duplicate().apply { rewind() }
            val bb = ByteBufferL.allocate(KEY_BYTES)
            val n = min(KEY_BYTES, dup.remaining())
            bb.put(dup.slice().limit(n))
            while (bb.position < KEY_BYTES) bb.put(0)
            bb.flip()
            return bb.asReadOnly()
        }
    }
}

