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
@file:Suppress("NOTHING_TO_INLINE", "unused", "DuplicatedCode")

package dev.swiftstorm.akkaradb.common

import dev.swiftstorm.akkaradb.common.types.U32
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.common.vh.LE
import java.nio.ByteBuffer
import kotlin.math.min

/**
 * AKHdr32 — AkkaraDB-specific 32-byte fixed header codec + key helpers.
 *
 * Layout (offset / size):
 *  0   2   kLen    (u16)
 *  2   4   vLen    (u32) — value length of *this chunk*
 *  6   8   seq     (u64)
 *  14  1   flags   (u8)
 *  15  1   pad0    (=0)
 *  16  8   keyFP64 (u64) — SipHash-2-4(key, DEFAULT_SIPHASH_SEED)
 *  24  8   miniKey (u64) — first <=8B of key, LE-packed (rest zero)
 *  -- total 32 bytes
 *
 * Spec / Generation policy:
 *  - Endianness: all fields are Little-Endian via LE.* primitives.
 *  - miniKey: buildMiniKeyLE(key) packs key[0]..key[<=7] into bits [7:0]..[63:56];
 *    short keys are zero-padded toward high bits.
 *  - keyFP64: SipHash-2-4 over the raw key bytes with k0 = DEFAULT_SIPHASH_SEED,
 *    k1 = (DEFAULT_SIPHASH_SEED xor 0x9E3779B97F4A7C15). Use as a 64-bit fingerprint
 *    for indexing, Bloom seeding, and fast pre-checks. Collisions are possible and
 *    must be resolved by full key compare on the reader side.
 */
object AKHdr32 {

    /** Default deterministic seed for SipHash-2-4 used to derive keyFP64. */
    val DEFAULT_SIPHASH_SEED: U64 = U64.fromSigned(0x5AD6DCD676D23C25L)

    const val SIZE = 32
    const val OFF_KLEN = 0
    const val OFF_VLEN = 2
    const val OFF_SEQ = 6
    const val OFF_FLAGS = 14
    const val OFF_PAD0 = 15
    const val OFF_KFP = 16
    const val OFF_MINI = 24

    data class Header(
        val kLen: Int,     // u16 (0..65535)
        val vLen: U32,     // u32
        val seq: U64,      // u64
        val flags: Int,    // u8 (0..255)
        val keyFP64: U64,  // u64
        val miniKey: U64   // u64
    )

    // -------- Write / Read --------

    /** Absolute write of 32-byte header at [at]. */
    @JvmStatic
    fun write(
        buf: ByteBuffer, at: Int,
        kLen: Int, vLen: U32, seq: U64, flags: Int,
        keyFP64: U64, miniKey: U64
    ) {
        require(kLen in 0..0xFFFF) { "kLen out of range: $kLen" }
        require(flags in 0..0xFF) { "flags out of range: $flags" }
        require(at >= 0 && at + SIZE <= buf.capacity()) { "header out of bounds: at=$at cap=${buf.capacity()}" }

        LE.putShort(buf, at + OFF_KLEN, (kLen and 0xFFFF).toShort())
        LE.putU32(buf, at + OFF_VLEN, vLen)
        LE.putU64(buf, at + OFF_SEQ, seq)
        LE.putU8(buf, at + OFF_FLAGS, flags)
        LE.putU8(buf, at + OFF_PAD0, 0)
        LE.putU64(buf, at + OFF_KFP, keyFP64)
        LE.putU64(buf, at + OFF_MINI, miniKey)
    }

    /** Relative write: writes at current position and advances by 32. */
    @JvmStatic
    fun writeRel(
        buf: ByteBuffer,
        kLen: Int, vLen: U32, seq: U64, flags: Int,
        keyFP64: U64, miniKey: U64
    ) {
        val p = buf.position()
        write(buf, p, kLen, vLen, seq, flags, keyFP64, miniKey)
        buf.position(p + SIZE)
    }

    /** Absolute read of 32-byte header from [at]. */
    @JvmStatic
    fun read(buf: ByteBuffer, at: Int): Header {
        require(at >= 0 && at + SIZE <= buf.capacity()) { "header out of bounds: at=$at cap=${buf.capacity()}" }
        val kLen = (LE.getShort(buf, at + OFF_KLEN).toInt() and 0xFFFF)
        val vLen = LE.getU32(buf, at + OFF_VLEN)
        val seq = LE.getU64(buf, at + OFF_SEQ)
        val flags = LE.getU8(buf, at + OFF_FLAGS)
        val kfp = LE.getU64(buf, at + OFF_KFP)
        val mini = LE.getU64(buf, at + OFF_MINI)
        return Header(kLen, vLen, seq, flags, kfp, mini)
    }

    /** Relative read: reads at current position and advances by 32. */
    @JvmStatic
    fun readRel(buf: ByteBuffer): Header {
        val p = buf.position()
        val h = read(buf, p)
        buf.position(p + SIZE)
        return h
    }

    // -------- Key helpers (DB-specific) --------

    /** Pack first up to 8 bytes of [key] into a little-endian 64-bit word (key[0] -> bits 7:0). */
    @JvmStatic
    fun buildMiniKeyLE(key: ByteBufferL): U64 {
        val dup = key.asReadOnlyDuplicate()
        val cnt = minOf(8, dup.remaining)
        var x = 0L
        var i = 0
        while (i < cnt) {
            val b = dup.i8 // relative LE-safe read, returns 0..255
            x = x or ((b.toLong() and 0xFFL) shl (8 * i))
            i++
        }
        return U64.fromSigned(x)
    }

    /** Pack first up to 8 bytes of [key] into a little-endian 64-bit word (key[0] -> bits 7:0). */
    @JvmStatic
    fun buildMiniKeyLE(key: ByteArray): U64 {
        val n = min(8, key.size)
        var x = 0L
        var i = 0
        while (i < n) {
            x = x or ((key[i].toLong() and 0xFFL) shl (8 * i))
            i++
        }
        return U64.fromSigned(x)
    }

    @JvmStatic
    fun sipHash24(key: ByteBufferL, seed64: U64): U64 {
        val k0 = seed64.raw
        val k1 = seed64.raw xor 0x9E3779B97F4A7C15uL.toLong()

        var v0 = 0x736f6d6570736575L xor k0
        var v1 = 0x646f72616e646f6dL xor k1
        var v2 = 0x6c7967656e657261L xor k0
        var v3 = 0x7465646279746573L xor k1

        // duplicate buffer without modifying original position/limit
        val bb = key.buf.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN)
        val n = bb.remaining()

        // ---- process full 8-byte blocks ----
        while (bb.remaining() >= 8) {
            val mi = bb.long  // LE (ByteBufferL guarantees LITTLE_ENDIAN)
            v3 = v3 xor mi

            repeat(2) {
                v0 += v1; v2 += v3
                v1 = (v1 shl 13) or (v1 ushr 51); v3 = (v3 shl 16) or (v3 ushr 48)
                v1 = v1 xor v0; v3 = v3 xor v2
                v0 = (v0 shl 32) or (v0 ushr 32)
                v2 += v1; v0 += v3
                v1 = (v1 shl 17) or (v1 ushr 47); v3 = (v3 shl 21) or (v3 ushr 43)
                v1 = v1 xor v2; v3 = v3 xor v0
                v2 = (v2 shl 32) or (v2 ushr 32)
            }

            v0 = v0 xor mi
        }

        // ---- last 1..7 bytes (LE, from the END) ----
        var b = (n.toLong() and 0xFF) shl 56
        val rem = bb.remaining()

        if (rem > 0) b = b or ((bb.get(bb.position() + rem - 1).toLong() and 0xFF) shl 0)
        if (rem > 1) b = b or ((bb.get(bb.position() + rem - 2).toLong() and 0xFF) shl 8)
        if (rem > 2) b = b or ((bb.get(bb.position() + rem - 3).toLong() and 0xFF) shl 16)
        if (rem > 3) b = b or ((bb.get(bb.position() + rem - 4).toLong() and 0xFF) shl 24)
        if (rem > 4) b = b or ((bb.get(bb.position() + rem - 5).toLong() and 0xFF) shl 32)
        if (rem > 5) b = b or ((bb.get(bb.position() + rem - 6).toLong() and 0xFF) shl 40)
        if (rem > 6) b = b or ((bb.get(bb.position() + rem - 7).toLong() and 0xFF) shl 48)

        // ---- final compression ----
        v3 = v3 xor b
        repeat(2) {
            v0 += v1; v2 += v3
            v1 = (v1 shl 13) or (v1 ushr 51); v3 = (v3 shl 16) or (v3 ushr 48)
            v1 = v1 xor v0; v3 = v3 xor v2
            v0 = (v0 shl 32) or (v0 ushr 32)
            v2 += v1; v0 += v3
            v1 = (v1 shl 17) or (v1 ushr 47); v3 = (v3 shl 21) or (v3 ushr 43)
            v1 = v1 xor v2; v3 = v3 xor v0
            v2 = (v2 shl 32) or (v2 ushr 32)
        }
        v0 = v0 xor b

        v2 = v2 xor 0xFF
        repeat(4) {
            v0 += v1; v2 += v3
            v1 = (v1 shl 13) or (v1 ushr 51); v3 = (v3 shl 16) or (v3 ushr 48)
            v1 = v1 xor v0; v3 = v3 xor v2
            v0 = (v0 shl 32) or (v0 ushr 32)
            v2 += v1; v0 += v3
            v1 = (v1 shl 17) or (v1 ushr 47); v3 = (v3 shl 21) or (v3 ushr 43)
            v1 = v1 xor v2; v3 = v3 xor v0
            v2 = (v2 shl 32) or (v2 ushr 32)
        }

        return U64.fromSigned(v0 xor v1 xor v2 xor v3)
    }

    /** SipHash-2-4(key, seed64) → 64-bit fingerprint (k0=seed, k1=seed^phi). */
    @JvmStatic
    fun sipHash24(key: ByteArray, seed64: U64): U64 {
        val k0 = seed64.raw
        val k1 = seed64.raw xor 0x9E3779B97F4A7C15uL.toLong() // golden ratio φ
        return sipHash24(key, U64.fromSigned(k0), U64.fromSigned(k1))
    }

    /** SipHash-2-4(key, (k0,k1)). */
    @JvmStatic
    fun sipHash24(m: ByteArray, k0: U64, k1: U64): U64 {
        var v0 = 0x736f6d6570736575L xor k0.raw
        var v1 = 0x646f72616e646f6dL xor k1.raw
        var v2 = 0x6c7967656e657261L xor k0.raw
        var v3 = 0x7465646279746573L xor k1.raw

        var i = 0
        val n = m.size
        while (i + 8 <= n) {
            val mi = (m[i].toLong() and 0xFF) or
                    ((m[i + 1].toLong() and 0xFF) shl 8) or
                    ((m[i + 2].toLong() and 0xFF) shl 16) or
                    ((m[i + 3].toLong() and 0xFF) shl 24) or
                    ((m[i + 4].toLong() and 0xFF) shl 32) or
                    ((m[i + 5].toLong() and 0xFF) shl 40) or
                    ((m[i + 6].toLong() and 0xFF) shl 48) or
                    ((m[i + 7].toLong() and 0xFF) shl 56)
            i += 8
            v3 = v3 xor mi
            repeat(2) {
                v0 += v1; v2 += v3
                v1 = (v1 shl 13) or (v1 ushr 51); v3 = (v3 shl 16) or (v3 ushr 48)
                v1 = v1 xor v0; v3 = v3 xor v2
                v0 = (v0 shl 32) or (v0 ushr 32)
                v2 += v1; v0 += v3
                v1 = (v1 shl 17) or (v1 ushr 47); v3 = (v3 shl 21) or (v3 ushr 43)
                v1 = v1 xor v2; v3 = v3 xor v0
                v2 = (v2 shl 32) or (v2 ushr 32)
            }
            v0 = v0 xor mi
        }

        var b = (n.toLong() and 0xFF) shl 56
        when (n and 7) {
            7 -> b = b or ((m[n - 7].toLong() and 0xFFL)) or
                    ((m[n - 6].toLong() and 0xFFL) shl 8) or
                    ((m[n - 5].toLong() and 0xFFL) shl 16) or
                    ((m[n - 4].toLong() and 0xFFL) shl 24) or
                    ((m[n - 3].toLong() and 0xFFL) shl 32) or
                    ((m[n - 2].toLong() and 0xFFL) shl 40) or
                    ((m[n - 1].toLong() and 0xFFL) shl 48)

            6 -> b = b or ((m[n - 6].toLong() and 0xFFL)) or
                    ((m[n - 5].toLong() and 0xFFL) shl 8) or
                    ((m[n - 4].toLong() and 0xFFL) shl 16) or
                    ((m[n - 3].toLong() and 0xFFL) shl 24) or
                    ((m[n - 2].toLong() and 0xFFL) shl 32) or
                    ((m[n - 1].toLong() and 0xFFL) shl 40)

            5 -> b = b or ((m[n - 5].toLong() and 0xFFL)) or
                    ((m[n - 4].toLong() and 0xFFL) shl 8) or
                    ((m[n - 3].toLong() and 0xFFL) shl 16) or
                    ((m[n - 2].toLong() and 0xFFL) shl 24) or
                    ((m[n - 1].toLong() and 0xFFL) shl 32)

            4 -> b = b or ((m[n - 4].toLong() and 0xFFL)) or
                    ((m[n - 3].toLong() and 0xFFL) shl 8) or
                    ((m[n - 2].toLong() and 0xFFL) shl 16) or
                    ((m[n - 1].toLong() and 0xFFL) shl 24)

            3 -> b = b or ((m[n - 3].toLong() and 0xFFL)) or
                    ((m[n - 2].toLong() and 0xFFL) shl 8) or
                    ((m[n - 1].toLong() and 0xFFL) shl 16)

            2 -> b = b or ((m[n - 2].toLong() and 0xFFL)) or
                    ((m[n - 1].toLong() and 0xFFL) shl 8)

            1 -> b = b or (m[n - 1].toLong() and 0xFFL)
            0 -> {}
        }

        v3 = v3 xor b
        repeat(2) {
            v0 += v1; v2 += v3
            v1 = (v1 shl 13) or (v1 ushr 51); v3 = (v3 shl 16) or (v3 ushr 48)
            v1 = v1 xor v0; v3 = v3 xor v2
            v0 = (v0 shl 32) or (v0 ushr 32)
            v2 += v1; v0 += v3
            v1 = (v1 shl 17) or (v1 ushr 47); v3 = (v3 shl 21) or (v3 ushr 43)
            v1 = v1 xor v2; v3 = v3 xor v0
            v2 = (v2 shl 32) or (v2 ushr 32)
        }
        v0 = v0 xor b

        v2 = v2 xor 0xFF
        repeat(4) {
            v0 += v1; v2 += v3
            v1 = (v1 shl 13) or (v1 ushr 51); v3 = (v3 shl 16) or (v3 ushr 48)
            v1 = v1 xor v0; v3 = v3 xor v2
            v0 = (v0 shl 32) or (v0 ushr 32)
            v2 += v1; v0 += v3
            v1 = (v1 shl 17) or (v1 ushr 47); v3 = (v3 shl 21) or (v3 ushr 43)
            v1 = v1 xor v2; v3 = v3 xor v0
            v2 = (v2 shl 32) or (v2 ushr 32)
        }
        return U64.fromSigned(v0 xor v1 xor v2 xor v3)
    }
}
