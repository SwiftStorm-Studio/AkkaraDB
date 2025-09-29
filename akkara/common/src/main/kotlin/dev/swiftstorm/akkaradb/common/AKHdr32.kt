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

@file:Suppress("NOTHING_TO_INLINE", "unused")

package dev.swiftstorm.akkaradb.common

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
 *  16  8   keyFP64 (u64) — SipHash-2-4(key, seed)
 *  24  8   miniKey (u64) — first <=8B of key, LE-packed (rest zero)
 *  -- total 32 bytes
 *
 * Notes:
 *  - API accepts vLen as Long but enforces u32 range (0..0xFFFF_FFFF).
 *  - Uses LE.* primitives (VarHandle-based, always Little-Endian).
 */
object AKHdr32 {

    const val SIZE = 32
    const val OFF_KLEN = 0
    const val OFF_VLEN = 2
    const val OFF_SEQ = 6
    const val OFF_FLAGS = 14
    const val OFF_PAD0 = 15
    const val OFF_KFP = 16
    const val OFF_MINI = 24

    data class Header(
        val kLen: Int,
        val vLen: Int,       // stored as u32; exposed as Int (0..0xFFFF_FFFF fits Int positive range?)
        val seq: Long,
        val flags: Int,
        val keyFP64: Long,
        val miniKey: Long
    )

    /** Absolute write of 32-byte header at [at]. */
    @JvmStatic
    fun write(
        buf: ByteBuffer, at: Int,
        kLen: Int, vLen: Long, seq: Long, flags: Int,
        keyFP64: Long, miniKey: Long
    ) {
        require(kLen in 0..0xFFFF) { "kLen out of range" }
        require(vLen in 0..0xFFFF_FFFFL) { "vLen out of u32 range" }
        require(flags in 0..0xFF) { "flags out of range" }
        require(at >= 0 && at + SIZE <= buf.capacity()) { "header out of bounds" }

        LE.putShort(buf, at + OFF_KLEN, kLen.toShort())
        LE.putInt(buf, at + OFF_VLEN, vLen.toInt())
        LE.putLong(buf, at + OFF_SEQ, seq)
        LE.putU8(buf, at + OFF_FLAGS, flags)
        LE.putU8(buf, at + OFF_PAD0, 0)
        LE.putLong(buf, at + OFF_KFP, keyFP64)
        LE.putLong(buf, at + OFF_MINI, miniKey)
    }

    /** Relative write: writes at current position and advances by 32. */
    @JvmStatic
    fun writeRel(
        buf: ByteBuffer,
        kLen: Int, vLen: Long, seq: Long, flags: Int,
        keyFP64: Long, miniKey: Long
    ) {
        val p = buf.position()
        write(buf, p, kLen, vLen, seq, flags, keyFP64, miniKey)
        buf.position(p + SIZE)
    }

    /** Absolute read of 32-byte header from [at]. */
    @JvmStatic
    fun read(buf: ByteBuffer, at: Int): Header {
        require(at >= 0 && at + SIZE <= buf.capacity()) { "header out of bounds" }
        val kLen = (LE.getShort(buf, at + OFF_KLEN).toInt() and 0xFFFF)
        val vLen = LE.getInt(buf, at + OFF_VLEN) // (0..0xFFFF_FFFF) will show as signed Int; use toUInt() if needed
        val seq = LE.getLong(buf, at + OFF_SEQ)
        val flags = LE.getU8(buf, at + OFF_FLAGS)
        val kfp = LE.getLong(buf, at + OFF_KFP)
        val mini = LE.getLong(buf, at + OFF_MINI)
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
    fun buildMiniKeyLE(key: ByteArray): Long {
        val n = min(8, key.size)
        var x = 0L
        var i = 0
        while (i < n) {
            x = x or ((key[i].toLong() and 0xFFL) shl (8 * i))
            i++
        }
        return x
    }

    /** SipHash-2-4(key, seed64) → 64-bit fingerprint (k0=seed, k1=seed^phi). */
    @JvmStatic
    fun sipHash24(key: ByteArray, seed64: Long): Long {
        val k0 = seed64
        val k1 = seed64 xor 0x9E3779B97F4A7C15uL.toLong() // use unsigned literal
        return sipHash24(key, k0, k1)
    }

    /** SipHash-2-4(key, (k0,k1)). */
    @JvmStatic
    fun sipHash24(m: ByteArray, k0: Long, k1: Long): Long {
        var v0 = 0x736f6d6570736575L xor k0
        var v1 = 0x646f72616e646f6dL xor k1
        var v2 = 0x6c7967656e657261L xor k0
        var v3 = 0x7465646279746573L xor k1

        var i = 0;
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
            7 -> {
                b = b or ((m[n - 7].toLong() and 0xFFL)) or
                        ((m[n - 6].toLong() and 0xFFL) shl 8) or
                        ((m[n - 5].toLong() and 0xFFL) shl 16) or
                        ((m[n - 4].toLong() and 0xFFL) shl 24) or
                        ((m[n - 3].toLong() and 0xFFL) shl 32) or
                        ((m[n - 2].toLong() and 0xFFL) shl 40) or
                        ((m[n - 1].toLong() and 0xFFL) shl 48)
            }

            6 -> {
                b = b or ((m[n - 6].toLong() and 0xFFL)) or
                        ((m[n - 5].toLong() and 0xFFL) shl 8) or
                        ((m[n - 4].toLong() and 0xFFL) shl 16) or
                        ((m[n - 3].toLong() and 0xFFL) shl 24) or
                        ((m[n - 2].toLong() and 0xFFL) shl 32) or
                        ((m[n - 1].toLong() and 0xFFL) shl 40)
            }

            5 -> {
                b = b or ((m[n - 5].toLong() and 0xFFL)) or
                        ((m[n - 4].toLong() and 0xFFL) shl 8) or
                        ((m[n - 3].toLong() and 0xFFL) shl 16) or
                        ((m[n - 2].toLong() and 0xFFL) shl 24) or
                        ((m[n - 1].toLong() and 0xFFL) shl 32)
            }

            4 -> {
                b = b or ((m[n - 4].toLong() and 0xFFL)) or
                        ((m[n - 3].toLong() and 0xFFL) shl 8) or
                        ((m[n - 2].toLong() and 0xFFL) shl 16) or
                        ((m[n - 1].toLong() and 0xFFL) shl 24)
            }

            3 -> {
                b = b or ((m[n - 3].toLong() and 0xFFL)) or
                        ((m[n - 2].toLong() and 0xFFL) shl 8) or
                        ((m[n - 1].toLong() and 0xFFL) shl 16)
            }

            2 -> {
                b = b or ((m[n - 2].toLong() and 0xFFL)) or
                        ((m[n - 1].toLong() and 0xFFL) shl 8)
            }

            1 -> {
                b = b or (m[n - 1].toLong() and 0xFFL)
            }

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
        return v0 xor v1 xor v2 xor v3
    }
}
