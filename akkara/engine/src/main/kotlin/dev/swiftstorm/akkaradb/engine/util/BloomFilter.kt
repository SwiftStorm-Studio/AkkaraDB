package dev.swiftstorm.akkaradb.engine.util

import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.borrow
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import kotlin.math.ln
import kotlin.math.max

class BloomFilter private constructor(
    private val bits: Int,
    private val hashCount: Int,
    internal val bitset: LongArray,
) {
    constructor(expectedEntries: Int, falsePositiveRate: Double = 0.01)
            : this(
        bits = calcBits(expectedEntries, falsePositiveRate),
        hashCount = calcHashCount(expectedEntries, falsePositiveRate),
        bitset = LongArray((calcBits(expectedEntries, falsePositiveRate) + 63) / 64)
    )

    private val pool = Pools.io()

    /* ---------- public ops ---------- */

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

    /* ---------- (de)serialization ---------- */

    fun writeTo(ch: WritableByteChannel) {
        val byteBuf = ByteBuffer.wrap(ByteArray(bitset.size * 8)).also { bb ->
            bitset.forEachIndexed { i, v -> bb.putLong(i * 8, v) }
        }
        while (byteBuf.hasRemaining()) ch.write(byteBuf)
    }

    fun byteSize(): Int = bitset.size * 8

    companion object {
        /* ---- Channel → BloomFilter ---- */
        fun readFrom(ch: ReadableByteChannel, size: Int): BloomFilter =
            Pools.io().borrow(size) { tmp ->
                tmp.clear()
                ch.read(tmp)          // full read
                tmp.flip()
                readFrom(tmp, size)
            }

        /* ---- ByteBuffer → BloomFilter ---- */
        fun readFrom(buf: ByteBuffer, size: Int? = null): BloomFilter {
            val actualSize = size ?: buf.remaining()
            require(actualSize % 8 == 0) { "bitset size must be multiple of 8" }

            val longs = LongArray(actualSize / 8)
            for (i in longs.indices) longs[i] = buf.long
            val bits = longs.size * 64

            val approxEntries = max(1, bits / 10)
            val hashCount = calcHashCount(approxEntries, 0.01)

            return BloomFilter(bits, hashCount, longs)
        }

        /* ---------- internal helpers ---------- */

        private const val LN2_SQ = 0.4804530139182014   // (ln 2)^2

        private fun calcBits(n: Int, p: Double): Int =
            (-n * ln(p) / LN2_SQ).toInt().coerceAtLeast(64)

        private fun calcHashCount(n: Int, p: Double): Int =
            max(2, (calcBits(n, p).toDouble() / n * ln(2.0)).toInt())

        private fun hash(key: ByteBuffer): Pair<Int, Int> {
            val (low, high) = murmur3_128(key)
            return Pair(low.toInt(), high.toInt())
        }

        /** Returns lower64 / upper64 packed into LongArray[2] */
        internal fun murmur3_128(buf: ByteBuffer, seed: Long = 0L): LongArray {
            var h1 = seed
            var h2 = seed
            val c1 = -0x783c846eeebdac2bL
            val c2 = 0x4cf5ad432745937fL

            val bb = buf.duplicate()
            val len = bb.remaining()
            val nBlocks = len ushr 4 // 16-byte blocks

            // main blocks
            for (i in 0 until nBlocks) {
                val k1 = bb.long
                val k2 = bb.long

                var k1m = k1 * c1
                k1m = java.lang.Long.rotateLeft(k1m, 31)
                k1m *= c2
                h1 = h1 xor k1m
                h1 = java.lang.Long.rotateLeft(h1, 27) + h2
                h1 = h1 * 5 + 0x52dce729

                var k2m = k2 * c2
                k2m = java.lang.Long.rotateLeft(k2m, 33)
                k2m *= c1
                h2 = h2 xor k2m
                h2 = java.lang.Long.rotateLeft(h2, 31) + h1
                h2 = h2 * 5 + 0x38495ab5
            }

            // tail
            var k1Tail = 0L
            var k2Tail = 0L
            val remaining = len and 15
            if (remaining > 0) {
                bb.position(nBlocks * 16)
                for (i in 0 until remaining) {
                    val v = (bb.get().toLong() and 0xffL)
                    when (i) {
                        0 -> k1Tail = k1Tail xor (v shl 0)
                        1 -> k1Tail = k1Tail xor (v shl 8)
                        2 -> k1Tail = k1Tail xor (v shl 16)
                        3 -> k1Tail = k1Tail xor (v shl 24)
                        4 -> k1Tail = k1Tail xor (v shl 32)
                        5 -> k1Tail = k1Tail xor (v shl 40)
                        6 -> k1Tail = k1Tail xor (v shl 48)
                        7 -> k1Tail = k1Tail xor (v shl 56)
                        8 -> k2Tail = k2Tail xor (v shl 0)
                        9 -> k2Tail = k2Tail xor (v shl 8)
                        10 -> k2Tail = k2Tail xor (v shl 16)
                        11 -> k2Tail = k2Tail xor (v shl 24)
                        12 -> k2Tail = k2Tail xor (v shl 32)
                        13 -> k2Tail = k2Tail xor (v shl 40)
                        14 -> k2Tail = k2Tail xor (v shl 48)
                    }
                }
            }

            if (k1Tail != 0L) {
                k1Tail *= c1
                k1Tail = java.lang.Long.rotateLeft(k1Tail, 31)
                k1Tail *= c2
                h1 = h1 xor k1Tail
            }

            if (k2Tail != 0L) {
                k2Tail *= c2
                k2Tail = java.lang.Long.rotateLeft(k2Tail, 33)
                k2Tail *= c1
                h2 = h2 xor k2Tail
            }

            // final mix
            h1 = h1 xor len.toLong()
            h2 = h2 xor len.toLong()
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
