package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.math.BigInteger
import kotlin.math.max

/**
 * Ultra-fast BigInteger adapter (LE-safe, no raw ByteBuffer).
 *
 * Binary layout:
 *   [int magLen] [mag bytes (abs(value), big-endian)] [u8 sign (255:-1, 0:0, 1:1)]
 *
 * Notes:
 * - magLen and scale-like ints are written via LE-safe i32.
 * - Magnitude is raw bytes (big-endian as returned by BigInteger.abs().toByteArray()).
 * - Sign is stored as an unsigned byte: -1 -> 255.
 */
object BigIntegerAdapter : TypeAdapter<BigInteger> {

    override fun estimateSize(value: BigInteger): Int {
        val magLen = max(1, (value.abs().bitLength() + 7) ushr 3) // exact unsigned byte length
        return 4 + magLen + 1 // i32:len + magnitude + u8:sign
    }

    override fun write(value: BigInteger, buffer: ByteBufferL) {
        val sign = value.signum() // -1, 0, 1
        val mag = value.abs().toByteArray()
        // Canonical zero as empty magnitude
        val magnitude = if (mag.size == 1 && mag[0].toInt() == 0) ByteArray(0) else mag

        // [int magLen]
        buffer.i32 = magnitude.size
        // [mag bytes]
        if (magnitude.isNotEmpty()) buffer.putBytes(magnitude)
        // [u8 sign] (-1 -> 255)
        buffer.i8 = if (sign == -1) 0xFF else sign and 0xFF
    }

    override fun read(buffer: ByteBufferL): BigInteger {
        // [int magLen]
        val magLen = buffer.i32
        // [mag bytes]
        val magnitude = ByteArray(magLen)
        var i = 0
        while (i < magLen) {
            magnitude[i] = buffer.i8.toByte()
            i++
        }
        // [u8 sign] (255 -> -1)
        val signU8 = buffer.i8
        val sign = when (signU8) {
            0 -> 0
            1 -> 1
            0xFF -> -1
            else -> if (signU8 >= 0x80) -1 else 1 // defensive fallback
        }

        return if (magLen == 0) BigInteger.ZERO else BigInteger(sign, magnitude)
    }
}