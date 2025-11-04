package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

object ShortAdapter : TypeAdapter<Short> {
    override fun estimateSize(value: Short) = 2

    override fun write(value: Short, buffer: ByteBufferL) {
        buffer.i16 = value
    }

    override fun read(buffer: ByteBufferL): Short = buffer.i16
}