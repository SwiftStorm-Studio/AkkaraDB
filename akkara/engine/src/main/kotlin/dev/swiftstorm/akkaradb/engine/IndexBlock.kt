package dev.swiftstorm.akkaradb.engine

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
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
    private val keys = ArrayList<ByteArray>()     // each 32 bytes
    private val offsets = ArrayList<Long>()

    fun add(firstKey: ByteBuffer, offset: Long) {
        val dup = firstKey.duplicate().apply { rewind() }
        val arr = ByteArray(KEY_BYTES)
        val n = min(KEY_BYTES, dup.remaining())
        dup.get(arr, 0, n) // remaining bytes are already 0
        keys += arr
        offsets += offset
    }

    /** Returns offset of the last block whose firstKey ≤ key, or -1 if none. */
    fun lookup(key: ByteBuffer): Long {
        val probe = prefix32(key)
        var lo = 0
        var hi = keys.lastIndex
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val cmp = compare32(keys[mid], probe)
            if (cmp <= 0) lo = mid + 1 else hi = mid - 1
        }
        return if (hi >= 0) offsets[hi] else -1
    }

    /* ───────── (de)serialization ───────── */

    fun writeTo(ch: WritableByteChannel) {
        val capacity = keys.size * (KEY_BYTES + java.lang.Long.BYTES)
        val buf = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN)
        for (i in keys.indices) {
            buf.put(keys[i])
            buf.putLong(offsets[i])
        }
        buf.flip(); ch.write(buf)
    }

    companion object {
        private const val KEY_BYTES = 32
        private const val ENTRY_BYTES = KEY_BYTES + java.lang.Long.BYTES

        fun readFrom(ch: ReadableByteChannel, size: Int): IndexBlock =
            ByteBuffer.allocate(size).also { ch.read(it); it.flip() }
                .let { readFrom(it, size) }

        fun readFrom(buf: ByteBuffer, size: Int): IndexBlock {
            require(size % ENTRY_BYTES == 0) { "invalid index size: $size" }
            val ib = IndexBlock()
            buf.order(ByteOrder.LITTLE_ENDIAN)
            val count = size / ENTRY_BYTES
            repeat(count) {
                val arr = ByteArray(KEY_BYTES)
                buf.get(arr)
                val off = buf.long
                ib.keys += arr
                ib.offsets += off
            }
            return ib
        }

        private fun prefix32(key: ByteBuffer): ByteArray {
            val d = key.duplicate().apply { rewind() }
            val arr = ByteArray(KEY_BYTES)
            val n = min(KEY_BYTES, d.remaining())
            d.get(arr, 0, n)
            return arr
        }

        private fun compare32(a: ByteArray, b: ByteArray): Int {
            var i = 0
            while (i < KEY_BYTES) {
                val x = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
                if (x != 0) return if (x < 0) -1 else 1
                i++
            }
            return 0
        }
    }
}
