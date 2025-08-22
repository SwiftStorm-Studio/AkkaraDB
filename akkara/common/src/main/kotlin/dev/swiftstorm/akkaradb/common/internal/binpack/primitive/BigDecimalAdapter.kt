package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * Ultra-fast BigDecimal adapter (fixed-length headers, no compression)
 * Layout:
 *   [int magLen] [mag bytes (abs(unscaled), big-endian)] [byte sign (-1,0,1)] [int scale]
 */
object BigDecimalAdapter : TypeAdapter<BigDecimal> {

    override fun estimateSize(value: BigDecimal): Int {
        val unscaled = value.unscaledValue()
        val magLen = max(1, (unscaled.abs().bitLength() + 7) ushr 3)
        return 4 + magLen + 1 + 4
    }

    override fun write(value: BigDecimal, buffer: ByteBuffer) {
        val unscaled = value.unscaledValue()
        val sign = unscaled.signum()
        val mag = unscaled.abs().toByteArray()
        val magnitude = if (mag.size == 1 && mag[0].toInt() == 0) ByteArray(0) else mag

        buffer.putInt(magnitude.size)
        buffer.put(magnitude)
        buffer.put(sign.toByte())
        buffer.putInt(value.scale())
    }

    override fun read(buffer: ByteBuffer): BigDecimal {
        val magLen = buffer.int
        val magnitude = ByteArray(magLen)
        if (magLen > 0) buffer.get(magnitude)
        val sign = buffer.get().toInt().coerceIn(-1, 1)
        val scale = buffer.int

        val unscaled = if (magLen == 0) BigInteger.ZERO else BigInteger(sign, magnitude)
        return BigDecimal(unscaled, scale)
    }
}
