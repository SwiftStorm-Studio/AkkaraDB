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

/** Unsigned 32-bit integer (zero-overhead, wraps Int). */
@JvmInline
value class U32(val raw: Int) : Comparable<U32> {

    companion object {
        /** Create from [0 .. 0xFFFF_FFFF]. */
        @JvmStatic
        fun of(value: Long): U32 {
            require(value in 0..0xFFFF_FFFFL) { "u32 out of range: $value" }
            return U32(value.toInt())
        }

        /** Construct from signed Int bits (e.g., read from ByteBuffer). */
        @JvmStatic
        fun fromSigned(raw: Int): U32 = U32(raw)

        // Constants: static getters（@JvmFieldは使わない）
        private val ZERO_CONST = U32(0)
        private val MAX_CONST = U32(-1) // 0xFFFF_FFFF

        @JvmStatic
        val ZERO: U32 get() = ZERO_CONST
        @JvmStatic
        val MAX: U32 get() = MAX_CONST
    }

    /** Unsigned long view [0..4294967295]. */
    inline fun toLong(): Long = java.lang.Integer.toUnsignedLong(raw)

    /** Downcast to Int if it fits [0..Int.MAX_VALUE]. */
    inline fun toIntExact(): Int {
        require(raw >= 0) { "does not fit into signed Int: ${toLong()}" }
        return raw
    }

    // arithmetic (mod 2^32)
    inline operator fun plus(other: U32): U32 = U32(this.raw + other.raw)
    inline operator fun minus(other: U32): U32 = U32(this.raw - other.raw)
    inline operator fun times(other: U32): U32 = U32(this.raw * other.raw)
    inline operator fun div(other: U32): U32 =
        U32(java.lang.Integer.divideUnsigned(this.raw, other.raw))

    inline operator fun rem(other: U32): U32 =
        U32(java.lang.Integer.remainderUnsigned(this.raw, other.raw))

    inline operator fun inc(): U32 = U32(this.raw + 1)
    inline operator fun dec(): U32 = U32(this.raw - 1)

    // bit ops
    inline infix fun and(other: U32): U32 = U32(this.raw and other.raw)
    inline infix fun or(other: U32): U32 = U32(this.raw or other.raw)
    inline infix fun xor(other: U32): U32 = U32(this.raw xor other.raw)
    inline fun inv(): U32 = U32(this.raw.inv())
    inline infix fun shl(bits: Int): U32 = U32(this.raw shl bits)
    inline infix fun shr(bits: Int): U32 = U32(this.raw ushr bits) // logical

    override fun compareTo(other: U32): Int =
        java.lang.Integer.compareUnsigned(this.raw, other.raw)

    override fun toString(): String =
        java.lang.Integer.toUnsignedString(raw)
}
