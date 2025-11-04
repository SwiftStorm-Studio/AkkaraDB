package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

object BooleanAdapter : TypeAdapter<Boolean> {
    override fun estimateSize(value: Boolean) = 1

    override fun write(value: Boolean, buffer: ByteBufferL) {
        buffer.i8 = if (value) 1 else 0
    }

    override fun read(buffer: ByteBufferL): Boolean {
        return buffer.i8 != 0
    }
}