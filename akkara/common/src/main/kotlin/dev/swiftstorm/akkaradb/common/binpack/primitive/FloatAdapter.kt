package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.lang.Float.floatToRawIntBits
import java.lang.Float.intBitsToFloat

object FloatAdapter : TypeAdapter<Float> {
    override fun estimateSize(value: Float) = 4

    override fun write(value: Float, buffer: ByteBufferL) {
        buffer.i32 = floatToRawIntBits(value)
    }

    override fun read(buffer: ByteBufferL): Float =
        intBitsToFloat(buffer.i32)
}