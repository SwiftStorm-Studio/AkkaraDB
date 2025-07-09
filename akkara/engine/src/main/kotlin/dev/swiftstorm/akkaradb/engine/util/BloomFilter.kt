package dev.swiftstorm.akkaradb.engine.util

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.roundToInt

/**
 * Simple Bloom-Filter (double-hash) compatible with Google SStable 形式。
 *
 * * false-positive 率は [fpRate] （デフォルト 1%）
 * * Murmur3 128-bit を 2 つの 32-bit ハッシュに折りたたんで k 回 linearly probe
 * * スレッドセーフ：bitset 書込は CAS（`LongArray` 原子演算）ではなく
 *   “同ビット OR” なので競合しても安全
 *
 * ⚠️ **この実装は可変**：add() 後も mightContain() の結果が変化します。
 */
class BloomFilter private constructor(
    private val bits: Int,
    private val hashCount: Int,
    private val bitset: LongArray,
) {

    /* ───────── public API ───────── */

    fun add(key: ByteBuffer) {
        val (h1, h2) = hash(key)
        for (i in 0 until hashCount) {
            val idx = ((h1 + i * h2).ushr(1) and Int.MAX_VALUE) % bits
            bitset[idx ushr 6] = bitset[idx ushr 6] or (1L shl (idx and 63))
        }
    }

    fun mightContain(key: ByteBuffer): Boolean {
        val (h1, h2) = hash(key)
        for (i in 0 until hashCount) {
            val idx = ((h1 + i * h2).ushr(1) and Int.MAX_VALUE) % bits
            if ((bitset[idx ushr 6] and (1L shl (idx and 63))) == 0L) return false
        }
        return true
    }

    /* ───────── stats / io ───────── */

    fun byteSize(): Int = bitset.size * 8

    fun writeTo(ch: WritableByteChannel) {
        // LongArray → ByteBuffer(LE) で 0-copy write
        val buf = ByteBuffer.allocate(bitset.size * 8).order(ByteOrder.LITTLE_ENDIAN)
        buf.asLongBuffer().put(bitset)
        ch.write(buf.flip())
    }

    /* ───────── companion ───────── */

    companion object {

        /** Creates a BloomFilter sized for [expectedInsertions] keys @ [fpRate] FP. */
        operator fun invoke(expectedInsertions: Int, fpRate: Double = 0.01): BloomFilter {
            require(expectedInsertions > 0) { "expectedInsertions must be positive" }
            require(fpRate in 1e-9..0.5) { "fpRate out of range" }

            val bits = optimalBits(expectedInsertions, fpRate)
            val hashes = optimalHashes(bits, expectedInsertions)
            return BloomFilter(bits, hashes, LongArray((bits + 63) ushr 6))
        }

        /* ─── deserialization ─── */

        fun readFrom(buf: ByteBuffer): BloomFilter {
            require(buf.remaining() % 8 == 0) { "Bloom bitset size must be multiple of 8" }
            val longs = LongArray(buf.remaining() / 8) { buf.long }
            val bits = longs.size * 64
            val hashes = optimalHashes(bits, maxOf(1, bits / 10))
            return BloomFilter(bits, hashes, longs)
        }

        fun readFrom(ch: ReadableByteChannel, size: Int): BloomFilter {
            require(size % 8 == 0) { "Bloom bitset size must be multiple of 8" }
            val buf = ByteBuffer.allocate(size)
            while (buf.hasRemaining()) ch.read(buf)
            buf.flip()
            return readFrom(buf)
        }

        /* ─── parameter calculus ─── */

        private const val LN2 = 0.6931471805599453     // ln 2
        private const val LN2_SQ = LN2 * LN2           // (ln 2)^2

        private fun optimalBits(n: Int, p: Double): Int =
            ceil(-n * ln(p) / LN2_SQ).toInt().coerceAtLeast(64)

        private fun optimalHashes(mBits: Int, n: Int): Int =
            maxOf(2, (mBits / n.toDouble() * LN2).roundToInt())

        /* ─── hashing ─── */

        private fun hash(key: ByteBuffer): Pair<Int, Int> {
            val (lo, hi) = murmur3_128(key)
            return lo.toInt() to hi.toInt()
        }

        /**
         * Murmur3 128‐bit little-endian。
         * 速度最適化のため “16 bytes ごとに 128-bit mix” をそのまま実装。
         * （Google Guava / Agrona の実装を簡略化）
         */
        internal fun murmur3_128(bufOrig: ByteBuffer, seed: Long = 0L): LongArray {
            val buf = bufOrig.duplicate().order(ByteOrder.LITTLE_ENDIAN)
            var h1 = seed
            var h2 = seed
            val c1 = -0x783c846eeebdac2bL
            val c2 = -0x7a143588f3d8f9e3L

            while (buf.remaining() >= 16) {
                var k1 = buf.long
                var k2 = buf.long

                k1 *= c1; k1 = java.lang.Long.rotateLeft(k1, 31); k1 *= c2; h1 = h1 xor k1
                h1 = java.lang.Long.rotateLeft(h1, 27) + h2; h1 = h1 * 5 + 0x52dce729
                k2 *= c2; k2 = java.lang.Long.rotateLeft(k2, 33); k2 *= c1; h2 = h2 xor k2
                h2 = java.lang.Long.rotateLeft(h2, 31) + h1; h2 = h2 * 5 + 0x38495ab5
            }

            // tail
            var k1 = 0L
            var k2 = 0L
            when (buf.remaining()) {
                15 -> k2 = buf.get(14).toLong() shl 48
                14 -> k2 = k2 or (buf.get(13).toLong() and 0xff shl 40)
                13 -> k2 = k2 or (buf.get(12).toLong() and 0xff shl 32)
                12 -> k2 = k2 or (buf.getInt(8).toLong() and 0xffffffffL)
                11 -> k1 = buf.get(10).toLong() shl 48
                10 -> k1 = k1 or (buf.get(9).toLong() and 0xff shl 40)
                9 -> k1 = k1 or (buf.get(8).toLong() and 0xff shl 32)
                8 -> k1 = buf.getLong(0)
                7 -> k1 = k1 or (buf.get(6).toLong() and 0xff shl 48)
                6 -> k1 = k1 or (buf.get(5).toLong() and 0xff shl 40)
                5 -> k1 = k1 or (buf.get(4).toLong() and 0xff shl 32)
                4 -> k1 = k1 or (buf.getInt(0).toLong() and 0xffffffffL)
                3 -> k1 = k1 or (buf.get(2).toLong() and 0xff shl 16)
                2 -> k1 = k1 or (buf.get(1).toLong() and 0xff shl 8)
                1 -> k1 = k1 or (buf.get(0).toLong() and 0xff)
                0 -> Unit
            }
            if (k1 != 0L) {
                k1 *= c1; k1 = java.lang.Long.rotateLeft(k1, 31); k1 *= c2; h1 = h1 xor k1
            }
            if (k2 != 0L) {
                k2 *= c2; k2 = java.lang.Long.rotateLeft(k2, 33); k2 *= c1; h2 = h2 xor k2
            }

            // final mix
            h1 = h1 xor bufOrig.remaining().toLong()
            h2 = h2 xor bufOrig.remaining().toLong()
            h1 += h2
            h2 += h1
            h1 = fmix64(h1)
            h2 = fmix64(h2)
            h1 += h2
            h2 += h1

            return longArrayOf(h1, h2)
        }

        private fun fmix64(k: Long): Long {
            var kk = k
            kk = kk xor (kk ushr 33)
            kk *= -0xae502812aa7333L
            kk = kk xor (kk ushr 33)
            kk *= -0x3b314601e57a13adL
            kk = kk xor (kk ushr 33)
            return kk
        }
    }
}
