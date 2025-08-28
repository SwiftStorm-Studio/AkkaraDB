package dev.swiftstorm.akkaradb.engine.util

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.lang.Long.rotateLeft
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.roundToInt

/**
 * <h2>ImmutableBloomFilter</h2>
 * A write‑once / read‑many Bloom‑filter that is safe to share between threads or replicas.
 *
 *  * Use [Builder] to incrementally add keys, then call [Builder.build] to obtain an
 *    **immutable** filter instance.
 *  * The immutable instance exposes only [mightContain] and *never* mutates its
 *    internal bit‑set – perfect for snapshot replication or mmap sharing.
 *  * The on‑disk format is just the raw `LongArray` little‑endian; identical to
 *    the previous implementation for backward compatibility.
 */
class BloomFilter private constructor(
    private val bits: Int,
    internal val hashCount: Int,
    private val bitset: LongArray,
) {

    /* ───────── public API ───────── */

    fun mightContain(key: ByteBufferL): Boolean {
        if (bits == 0) return false              // safety guard – never /0
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
        val buf = ByteBuffer.allocate(byteSize()).order(ByteOrder.LITTLE_ENDIAN)
        buf.asLongBuffer().put(bitset)
        ch.write(buf.flip())
    }

    /* ───────── companion ───────── */
    companion object {
        operator fun invoke(expectedInsertions: Int, fpRate: Double = 0.01): Builder {
            return Builder(expectedInsertions, fpRate)
        }

        /** Deserialize from a mapped/read‑only [ByteBuffer]. */
        fun readFrom(buf: ByteBuffer, hashCount: Int): BloomFilter {
            require(buf.remaining() % 8 == 0) { "Bloom bitset size must be multiple of 8" }
            val longs = LongArray(buf.remaining() / 8) { buf.long }
            val bits = longs.size * 64
            return BloomFilter(bits, hashCount, longs)
        }

        /** Deserialize when the byte length is known but not mapped yet (e.g. FileChannel). */
        fun readFrom(ch: ReadableByteChannel, bytes: Int, hashCount: Int): BloomFilter {
            val buf = ByteBuffer.allocate(bytes)
            while (buf.hasRemaining()) ch.read(buf)
            buf.flip()
            return readFrom(buf, hashCount)
        }

        /* ─── hashing helpers (same Murmur3 impl as before) ─── */
        private fun hash(key: ByteBufferL): Pair<Int, Int> {
            val (lo, hi) = murmur3_128(key)
            return lo.toInt() to hi.toInt()
        }

        private fun murmur3_128(bufOrig: ByteBufferL, seed: Long = 0L): LongArray {
            val buf = bufOrig.duplicate()
            var h1 = seed
            var h2 = seed
            val c1 = -0x783c_846e_eebd_ac2bL
            val c2 = -0x7a14_3588_f3d8_f9e3L

            while (buf.remaining >= 16) {
                var k1 = buf.long
                var k2 = buf.long
                k1 *= c1; k1 = rotateLeft(k1, 31); k1 *= c2; h1 = h1 xor k1
                h1 = rotateLeft(h1, 27) + h2; h1 = h1 * 5 + 0x52dce729
                k2 *= c2; k2 = rotateLeft(k2, 33); k2 *= c1; h2 = h2 xor k2
                h2 = rotateLeft(h2, 31) + h1; h2 = h2 * 5 + 0x38495ab5
            }

            var k1 = 0L
            var k2 = 0L
            when (buf.remaining) {
                15 -> k2 = buf.get(14).toLong() shl 48
                14 -> k2 = k2 or (buf.get(13).toLong() and 0xff shl 40)
                13 -> k2 = k2 or (buf.get(12).toLong() and 0xff shl 32)
                12 -> k2 = k2 or (buf.getInt(8).toLong() and 0xffff_ffffL)
                11 -> k1 = buf.get(10).toLong() shl 48
                10 -> k1 = k1 or (buf.get(9).toLong() and 0xff shl 40)
                9 -> k1 = k1 or (buf.get(8).toLong() and 0xff shl 32)
                8 -> k1 = buf.getLong(0)
                7 -> k1 = k1 or (buf.get(6).toLong() and 0xff shl 48)
                6 -> k1 = k1 or (buf.get(5).toLong() and 0xff shl 40)
                5 -> k1 = k1 or (buf.get(4).toLong() and 0xff shl 32)
                4 -> k1 = k1 or (buf.getInt(0).toLong() and 0xffff_ffffL)
                3 -> k1 = k1 or (buf.get(2).toLong() and 0xff shl 16)
                2 -> k1 = k1 or (buf.get(1).toLong() and 0xff shl 8)
                1 -> k1 = k1 or (buf.get(0).toLong() and 0xff)
                0 -> Unit
            }
            if (k1 != 0L) {
                k1 *= c1; k1 = rotateLeft(k1, 31); k1 *= c2; h1 = h1 xor k1
            }
            if (k2 != 0L) {
                k2 *= c2; k2 = rotateLeft(k2, 33); k2 *= c1; h2 = h2 xor k2
            }
            h1 = h1 xor bufOrig.remaining.toLong(); h2 = h2 xor bufOrig.remaining.toLong(); h1 += h2; h2 += h1
            h1 = fmix64(h1); h2 = fmix64(h2); h1 += h2; h2 += h1
            return longArrayOf(h1, h2)
        }

        private fun fmix64(k: Long): Long {
            var kk = k
            kk = kk xor (kk ushr 33)
            kk *= -0xae50_2812_aa73_33L
            kk = kk xor (kk ushr 33)
            kk *= -0x3b31_4601_e57a_13adL
            kk = kk xor (kk ushr 33)
            return kk
        }
    }

    /* ───────── Builder ───────── */
    class Builder(expectedInsertions: Int, fpRate: Double = 0.01) {
        private val bits: Int
        private val hashCount: Int
        private val bitset: LongArray

        init {
            require(expectedInsertions > 0) { "expectedInsertions must be positive" }
            require(fpRate in 1e-9..0.5) { "fpRate out of range" }
            bits = optimalBits(expectedInsertions, fpRate)
            hashCount = optimalHashes(bits, expectedInsertions)
            bitset = LongArray((bits + 63) ushr 6)
        }

        fun add(key: ByteBufferL): Builder {
            val (h1, h2) = hash(key)
            for (i in 0 until hashCount) {
                val idx = ((h1 + i * h2).ushr(1) and Int.MAX_VALUE) % bits
                bitset[idx ushr 6] = bitset[idx ushr 6] or (1L shl (idx and 63))
            }
            return this
        }

        fun build(): BloomFilter = BloomFilter(bits, hashCount, bitset)

        /* reuse companion math */
        private fun optimalBits(n: Int, p: Double): Int = ceil(-n * ln(p) / LN2_SQ).toInt().coerceAtLeast(64)
        private fun optimalHashes(mBits: Int, n: Int): Int = max(2, (mBits / n.toDouble() * LN2).roundToInt())
    }
}

private const val LN2 = 0.6931471805599453
private const val LN2_SQ = LN2 * LN2