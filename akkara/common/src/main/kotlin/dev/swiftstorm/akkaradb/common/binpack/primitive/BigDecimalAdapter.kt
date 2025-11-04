package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.math.max

/**
 * Ultra-fast BigDecimal adapter (LE-safe, no raw ByteBuffer access)
 *
 * Binary layout:
 *   [int magLen] [mag bytes (abs(unscaled), big-endian)] [u8 sign (255:-1, 0:0, 1:1)] [int scale]
 */
object BigDecimalAdapter : TypeAdapter<BigDecimal> {

    override fun estimateSize(value: BigDecimal): Int {
        val unscaled = value.unscaledValue()
        val magLen = max(1, (unscaled.abs().bitLength() + 7) ushr 3)
        // 4 (magLen) + magLen + 1 (sign) + 4 (scale)
        return 4 + magLen + 1 + 4
    }

    override fun write(value: BigDecimal, buffer: ByteBufferL) {
        val unscaled = value.unscaledValue()
        val sign = unscaled.signum()              // -1, 0, 1
        val mag = unscaled.abs().toByteArray()
        // Canonical zero as empty magnitude
        val magnitude = if (mag.size == 1 && mag[0].toInt() == 0) ByteArray(0) else mag

        // [int magLen]
        buffer.i32 = magnitude.size
        // [mag bytes]
        if (magnitude.isNotEmpty()) {
            buffer.putBytes(magnitude)
        }
        // [u8 sign] (-1 -> 255)
        buffer.i8 = if (sign == -1) 0xFF else sign and 0xFF
        // [int scale]
        buffer.i32 = value.scale()
    }

    override fun read(buffer: ByteBufferL): BigDecimal {
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
        // [int scale]
        val scale = buffer.i32

        val unscaled = if (magLen == 0) BigInteger.ZERO else BigInteger(sign, magnitude)
        return BigDecimal(unscaled, scale)
    }
}
