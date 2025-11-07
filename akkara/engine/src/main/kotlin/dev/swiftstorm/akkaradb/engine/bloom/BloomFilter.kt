/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

package dev.swiftstorm.akkaradb.engine.bloom

import dev.swiftstorm.akkaradb.common.AKHdr32
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.types.U64
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.roundToInt

/**
 * ImmutableBloomFilter (v3)
 *
 * On-disk layout (LE):
 *   magic:u32 = 'AKBL' (0x414B424C)
 *   ver  :u8  = 1
 *   k    :u8  = hash count
 *   pad  :u16 = 0
 *   mBits:u32 = bit array size (power-of-two)
 *   seed :u64 = hashing seed (for key→fp64, optional)
 *   bits :u8[mBits/8] = bitset body (as 64-bit words)
 */
class BloomFilter private constructor(
    val mBits: Int,
    val k: Int,
    private val seed: Long,
    private val words: LongArray
) {
    val mask: Int = mBits - 1
    val byteSize: Int get() = words.size * 8
    val entryBits: Int get() = mBits

    /** Double hashing: idx_i = (h1 + i*h2) & mask */
    fun mightContainFp64(fp64: Long): Boolean {
        if (mBits == 0 || k == 0) return false
        val h1 = mix64(fp64)
        val h2 = mix64(h1 xor seed)
        var i = 0
        while (i < k) {
            val idx = ((h1 + i.toLong() * h2).toInt()) and mask
            val w = idx ushr 6
            val bit = 1L shl (idx and 63)
            if ((words[w] and bit) == 0L) return false
            i++
        }
        return true
    }

    fun mightContain(key: ByteBufferL): Boolean =
        mightContainFp64(fingerprint64(key, U64.fromSigned(seed)))

    /* ---------- Serialization ---------- */

    fun writeTo(ch: WritableByteChannel) {
        val headerBytes = HEADER_SIZE
        val bodyBytes = byteSize
        val buf = ByteBufferL.allocate(headerBytes + bodyBytes)

        // header (absolute writes)
        buf.at(OFF_MAGIC).i32 = MAGIC
        buf.at(OFF_VER).i8 = VER
        buf.at(OFF_K).i8 = k
        buf.at(OFF_PAD).i16 = 0
        buf.at(OFF_MBITS).u32 = dev.swiftstorm.akkaradb.common.types.U32.of(mBits.toLong())
        buf.at(OFF_SEED).i64 = seed

        // body: write per 8 bytes to avoid 8B-alignment requirement of putLongs()
        var p = HEADER_SIZE
        var i = 0
        while (i < words.size) {
            buf.at(p).i64 = words[i]
            p += 8
            i++
        }

        buf.position(0)
        buf.writeFully(ch, headerBytes + bodyBytes)
    }

    fun writeTo(dst: ByteBufferL) {
        val start = dst.position

        // header
        dst.at(start + OFF_MAGIC).i32 = MAGIC
        dst.at(start + OFF_VER).i8 = VER
        dst.at(start + OFF_K).i8 = k
        dst.at(start + OFF_PAD).i16 = 0
        dst.at(start + OFF_MBITS).u32 = dev.swiftstorm.akkaradb.common.types.U32.of(mBits.toLong())
        dst.at(start + OFF_SEED).i64 = seed

        // body: same reason, write per 8 bytes
        var p = start + HEADER_SIZE
        var i = 0
        while (i < words.size) {
            dst.at(p).i64 = words[i]
            p += 8
            i++
        }

        dst.position(start + HEADER_SIZE + byteSize)
    }

    companion object {
        private const val MAGIC = 0x414B424C // 'A''K''B''L'
        private const val VER = 1
        private const val OFF_MAGIC = 0
        private const val OFF_VER = 4
        private const val OFF_K = 5
        private const val OFF_PAD = 6
        private const val OFF_MBITS = 8
        private const val OFF_SEED = 12
        internal const val HEADER_SIZE = 20

        operator fun invoke(
            expectedInsertions: Long,
            fpRate: Double = 0.01,
            seed: U64 = U64.ZERO,
            roundBitsToPow2: Boolean = true
        ): Builder {
            require(expectedInsertions > 0) { "expectedInsertions must be positive" }
            require(fpRate in 1e-9..0.5) { "fpRate out of range" }

            val rawBits = optimalBits(expectedInsertions, fpRate)
            val mBits = if (roundBitsToPow2) roundUpPow2(rawBits) else rawBits.toInt().coerceAtLeast(64)
            var k = optimalHashes(mBits, expectedInsertions)

            // Safety clamp: never allow unrealistic hash counts
            if (k < 1) k = 1
            else if (k > 16) {
                // logarithmic degradation: cap at 16 but print a warning
                println("[BloomFilter] WARN: excessive k=$k for mBits=$mBits, entries=$expectedInsertions; clamped to 16")
                k = 16
            }

            return Builder(mBits, k, seed.raw)
        }

        fun readFrom(buf: ByteBufferL): BloomFilter {
            require(buf.remaining >= HEADER_SIZE) { "buffer too small for Bloom header" }
            val magic = buf.at(OFF_MAGIC).i32
            require(magic == MAGIC) { "invalid magic: 0x${magic.toString(16)}" }
            val ver = buf.at(OFF_VER).i8
            require(ver == VER) { "unsupported Bloom version: $ver" }
            val k = buf.at(OFF_K).i8
            require(k in 1..16) { "invalid k: $k" }
            val mBits = buf.at(OFF_MBITS).u32.toIntExact()
            val seed = buf.at(OFF_SEED).i64
            require(isPow2(mBits)) { "mBits must be power-of-two" }

            val bodyBytes = mBits ushr 3
            require(buf.remaining >= HEADER_SIZE + bodyBytes) { "bitset truncated" }
            val words = LongArray(bodyBytes / 8)
            var p = HEADER_SIZE
            var i = 0
            while (i < words.size) {
                words[i] = buf.at(p).i64
                p += 8
                i++
            }
            return BloomFilter(mBits, k, seed, words)
        }

        fun readFrom(ch: ReadableByteChannel, totalBytes: Int): BloomFilter {
            val tmp = ByteBufferL.allocate(totalBytes)
            tmp.readFully(ch, totalBytes)
            tmp.position(0).limit(totalBytes)
            return readFrom(tmp)
        }

        /* ---- Math / Hash helpers ---- */

        private const val LN2 = 0.6931471805599453
        private const val LN2_SQ = LN2 * LN2

        private fun optimalBits(n: Long, p: Double): Long =
            ceil(-n * ln(p) / LN2_SQ).toLong().coerceAtLeast(64)

        private fun optimalHashes(mBits: Int, n: Long): Int =
            max(2, (mBits / n.toDouble() * LN2).roundToInt())

        private fun isPow2(x: Int): Boolean = x > 0 && (x and (x - 1)) == 0
        private fun roundUpPow2(x: Long): Int {
            var v = x - 1
            v = v or (v shr 1)
            v = v or (v shr 2)
            v = v or (v shr 4)
            v = v or (v shr 8)
            v = v or (v shr 16)
            v = v or (v shr 32)
            return (v + 1).toInt()
        }

        private fun mix64(x: Long): Long {
            var z = x.toULong() + 0x9E3779B97F4A7C15uL
            z = (z xor (z shr 30)) * 0xBF58476D1CE4E5B9uL
            z = (z xor (z shr 27)) * 0x94D049BB133111EBuL
            z = z xor (z shr 31)
            return z.toLong()
        }

        private fun fingerprint64(key: ByteBufferL, seed64: U64): Long {
            val bb = key.duplicate()
            val out = ByteArray(bb.remaining)
            var i = 0
            while (bb.has()) {
                out[i++] = (bb.i8 and 0xFF).toByte()
            }
            return AKHdr32.sipHash24(out, seed64).raw
        }
    }

    /* ---------- Builder (mutable; write-once) ---------- */
    class Builder internal constructor(
        private val mBits: Int,
        private val k: Int,
        private val seed: Long
    ) {
        private val mask = mBits - 1
        private val words = LongArray(mBits ushr 6)

        fun addFp64(fp64: Long): Builder {
            val h1 = mix64(fp64)
            val h2 = mix64(h1 xor seed)
            var i = 0
            while (i < k) {
                val idx = ((h1 + i.toLong() * h2).toInt()) and mask
                val w = idx ushr 6
                val bit = 1L shl (idx and 63)
                words[w] = words[w] or bit
                i++
            }
            return this
        }

        fun addKey(key: ByteBufferL): Builder =
            addFp64(fingerprint64(key, U64.fromSigned(seed)))

        fun build(): BloomFilter = BloomFilter(mBits, k, seed, words)
    }
}
