@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.channels.WritableByteChannel
import kotlin.math.min

/**
 * IndexBlock (v3)
 *
 * Purpose:
 *   Stores the first key of each 32 KiB data block as a fixed 32-byte normalized key,
 *   allowing binary search over the sorted list to locate a target block quickly.
 *
 * On-disk layout (Little-Endian):
 * ```
 * magic:u32 = 'AKIX' (0x414B4958)
 * ver  :u8  = 1
 * kSize:u8  = 32              // fixed key length (v3 uses 32 B)
 * pad  :u16 = 0
 * count:u32                   // number of entries (= number of data blocks)
 * entries[count] {
 *   key[32]                   // firstKey, zero-padded or truncated
 *   blockOff:u64              // absolute offset of the data block
 * }
 * ```
 *
 * Reader side keeps a zero-copy ByteBufferL slice referencing the entry region.
 * Comparison is done directly via absolute reads; no GC objects are created.
 */
class IndexBlock private constructor(
    private val kSize: Int,
    private val count: Int,
    private val entriesSlice: ByteBufferL
) {
    init {
        require(kSize == KEY_BYTES) { "kSize != $KEY_BYTES is not supported (got $kSize)" }
        require(count >= 0)
        require(entriesSlice.remaining == count * ENTRY_BYTES) {
            "entries slice size mismatch"
        }
    }

    /** Number of entries (== number of data blocks). */
    fun size(): Int = count

    /**
     * Lower-bound search:
     *   returns the index of the first key >= [targetKey32].
     * Result range is [0, size].
     */
    fun lowerBound32(targetKey32: ByteArray): Int {
        require(targetKey32.size == KEY_BYTES)
        var lo = 0
        var hi = count
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            val cmp = compareKeyAt(mid, targetKey32)
            if (cmp < 0) lo = mid + 1 else hi = mid
        }
        return lo
    }

    /**
     * Lookup by normalized 32-byte key.
     * Returns the data-block offset that should contain the key,
     * or -1 if the target is before the first entry.
     */
    fun lookup32(targetKey32: ByteArray): Long {
        val idx = lowerBound32(targetKey32)
        if (idx == 0 && compareKeyAt(0, targetKey32) > 0) return -1L
        val hit = if (idx == count) count - 1 else idx
        return blockOffAt(hit)
    }

    /** Lookup for a variable-length key: internally normalized to 32 B. */
    fun lookup(key: ByteBufferL): Long = lookup32(normalize32(key))

    /** Returns the data-block offset at entry [i]. */
    fun blockOffAt(i: Int): Long {
        require(i in 0 until count)
        val base = i * ENTRY_BYTES
        val offPos = base + KEY_BYTES
        return entriesSlice.at(offPos).i64
    }

    /** Lexicographic comparison between entry[i].key and [targetKey32]. */
    private fun compareKeyAt(i: Int, targetKey32: ByteArray): Int {
        require(i in 0 until count)
        val base = i * ENTRY_BYTES
        var j = 0
        while (j < KEY_BYTES) {
            val a = entriesSlice.at(base + j).i8 and 0xFF
            val b = targetKey32[j].toInt() and 0xFF
            if (a != b) return a - b
            j++
        }
        return 0
    }

    companion object {
        private const val MAGIC = 0x414B4958 // 'A''K''I''X'
        private const val VER = 1
        private const val KEY_BYTES = 32
        private const val ENTRY_BYTES = KEY_BYTES + 8
        private const val OFF_MAGIC = 0
        private const val OFF_VER = 4
        private const val OFF_KSIZE = 5
        private const val OFF_PAD = 6
        private const val OFF_COUNT = 8
        private const val HEADER_SIZE = 12

        /** Parses an IndexBlock from [buf] starting at its magic header. */
        fun readFrom(buf: ByteBufferL): IndexBlock {
            require(buf.remaining >= HEADER_SIZE) { "index header too small" }
            val magic = buf.at(OFF_MAGIC).i32
            require(magic == MAGIC) { "bad index magic: 0x${magic.toString(16)}" }
            val ver = buf.at(OFF_VER).i8
            require(ver == VER) { "unsupported index ver=$ver" }
            val kSize = buf.at(OFF_KSIZE).i8
            val count = buf.at(OFF_COUNT).i32
            require(count >= 0)
            val entriesBytes = count * ENTRY_BYTES
            require(buf.remaining >= HEADER_SIZE + entriesBytes) { "index truncated" }

            val entries = buf.sliceAt(HEADER_SIZE, entriesBytes)
            return IndexBlock(kSize, count, entries)
        }

        /**
         * Normalize a variable-length key to a fixed 32-byte representation.
         * Short keys are zero-padded; long keys are truncated.
         */
        fun normalize32(key: ByteBufferL): ByteArray {
            val src = key.duplicate()
            val out = ByteArray(KEY_BYTES)
            val n = min(KEY_BYTES, src.remaining)
            var i = 0
            while (i < n) {
                out[i] = (src.i8 and 0xFF).toByte()
                i++
            }
            return out
        }
    }

    /* -------------------- Builder -------------------- */

    /**
     * Builder used by SSTableWriter.
     * Add an entry for each sealed data block (firstKey + block offset),
     * then call [buildBuffer] or [writeTo] once at the end.
     */
    class Builder(initialCapacity: Int = 256) {
        private val keys = ArrayList<ByteArray>(initialCapacity)
        private val offsets = ArrayList<Long>(initialCapacity)

        /** Adds a new entry with a variable-length key. */
        fun add(firstKey: ByteBufferL, blockOff: Long): Builder {
            keys += normalize32(firstKey)
            offsets += blockOff
            return this
        }

        /** Adds a pre-normalized 32-byte key (fast path). */
        fun addKey32(firstKey32: ByteArray, blockOff: Long): Builder {
            require(firstKey32.size == KEY_BYTES)
            keys += firstKey32.copyOf()
            offsets += blockOff
            return this
        }

        /**
         * Serializes the index into a contiguous ByteBufferL.
         * The resulting buffer can be appended to the end of an SSTable.
         */
        fun buildBuffer(): ByteBufferL {
            val count = keys.size
            val size = HEADER_SIZE + count * ENTRY_BYTES
            val buf = ByteBufferL.allocate(size)

            // header
            buf.at(OFF_MAGIC).i32 = MAGIC
            buf.at(OFF_VER).i8 = VER
            buf.at(OFF_KSIZE).i8 = KEY_BYTES
            buf.at(OFF_PAD).i16 = 0
            buf.at(OFF_COUNT).i32 = count

            // entries (absolute writes)
            var p = HEADER_SIZE
            var i = 0
            while (i < count) {
                val k = keys[i]
                var j = 0
                while (j < KEY_BYTES) {
                    buf.at(p + j).i8 = k[j].toInt() and 0xFF
                    j++
                }
                p += KEY_BYTES
                buf.at(p).i64 = offsets[i]
                p += 8
                i++
            }

            buf.position(size)
            return buf
        }

        /** Writes the serialized index directly to a channel. */
        fun writeTo(ch: WritableByteChannel): Int {
            val tmp = buildBuffer()
            tmp.position(0)
            return tmp.writeFully(ch, tmp.remaining)
        }
    }
}
