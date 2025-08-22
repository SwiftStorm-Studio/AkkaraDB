package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.math.BigInteger
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * Ultra-fast BigInteger adapter
 * Layout:
 *   [int magLen] [mag bytes (abs(value), big-endian)] [byte sign (-1,0,1)]
 */
object BigIntegerAdapter : TypeAdapter<BigInteger> {

    override fun estimateSize(value: BigInteger): Int {
        val magLen = max(1, (value.abs().bitLength() + 7) ushr 3) // 厳密バイト数 (unsigned)
        return 4 + magLen + 1 // int:len + magnitude + byte:sign
    }

    override fun write(value: BigInteger, buffer: ByteBuffer) {
        val sign = value.signum()
        val mag = value.abs().toByteArray()
        val magnitude = if (mag.size == 1 && mag[0].toInt() == 0) ByteArray(0) else mag

        buffer.putInt(magnitude.size)
        buffer.put(magnitude)
        buffer.put(sign.toByte())
    }

    override fun read(buffer: ByteBuffer): BigInteger {
        val magLen = buffer.int
        val magnitude = ByteArray(magLen)
        if (magLen > 0) buffer.get(magnitude)
        val sign = buffer.get().toInt().coerceIn(-1, 1)

        return if (magLen == 0) BigInteger.ZERO else BigInteger(sign, magnitude)
    }
}
