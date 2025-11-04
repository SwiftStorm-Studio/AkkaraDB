package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

object IntAdapter : TypeAdapter<Int> {
    override fun estimateSize(value: Int) = 4

    override fun write(value: Int, buffer: ByteBufferL) {
        buffer.i32 = value
    }

    override fun read(buffer: ByteBufferL): Int = buffer.i32
}