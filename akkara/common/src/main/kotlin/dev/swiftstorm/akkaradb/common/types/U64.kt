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
@file:Suppress("NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.common.types

/** Unsigned 64-bit integer (zero-overhead, wraps Long). */
@JvmInline
value class U64(val raw: Long) : Comparable<U64> {

    companion object {
        /** Construct from raw signed Long bits (e.g., read from ByteBuffer). */
        @JvmStatic
        fun fromSigned(raw: Long): U64 = U64(raw)

        private val ZERO_CONST = U64(0L)
        private val MAX_CONST = U64(-1L) // 0xFFFF_FFFF_FFFF_FFFFL

        @JvmStatic
        val ZERO: U64 get() = ZERO_CONST
        @JvmStatic
        val MAX: U64 get() = MAX_CONST

        /** Parse unsigned decimal (fast path, digits only). */
        @JvmStatic
        fun parseUnsignedDecimal(s: String): U64 {
            var hi = 0L
            var lo = 0L
            for (ch in s) {
                val d = ch.code - 48
                require(d in 0..9) { "invalid u64: $s" }
                // (hi,lo) = (hi,lo) * 10 + d  in 128-bit then take low 64b
                // lo = lo*10 + d; hi = hi*10 + carry; carry = (lo before << 3 + << 1) overflow…
                val loMul10 = (lo shl 3) + (lo shl 1)
                val newLo = loMul10 + d
                val carry1 = if (java.lang.Long.compareUnsigned(newLo, loMul10) < 0) 1 else 0
                val hiMul10 = (hi shl 3) + (hi shl 1)
                val newHi = hiMul10 + carry1
                hi = newHi
                lo = newLo
            }
            return U64(lo) // hi is dropped mod 2^64
        }
    }

    override fun toString(): String = java.lang.Long.toUnsignedString(raw)

    // arithmetic (mod 2^64)
    inline operator fun plus(other: U64): U64 = U64(this.raw + other.raw)
    inline operator fun minus(other: U64): U64 = U64(this.raw - other.raw)
    inline operator fun times(other: U64): U64 = U64(this.raw * other.raw)
    inline operator fun div(other: U64): U64 =
        U64(java.lang.Long.divideUnsigned(this.raw, other.raw))

    inline operator fun rem(other: U64): U64 =
        U64(java.lang.Long.remainderUnsigned(this.raw, other.raw))

    inline operator fun inc(): U64 = U64(this.raw + 1)
    inline operator fun dec(): U64 = U64(this.raw - 1)

    // bit ops
    inline infix fun and(other: U64): U64 = U64(this.raw and other.raw)
    inline infix fun or(other: U64): U64 = U64(this.raw or other.raw)
    inline infix fun xor(other: U64): U64 = U64(this.raw xor other.raw)
    inline fun inv(): U64 = U64(this.raw.inv())
    inline infix fun shl(bits: Int): U64 = U64(this.raw shl bits)
    inline infix fun shr(bits: Int): U64 = U64(this.raw ushr bits) // logical

    override fun compareTo(other: U64): Int =
        java.lang.Long.compareUnsigned(this.raw, other.raw)
}
