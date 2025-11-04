package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

object LongAdapter : TypeAdapter<Long> {
    override fun estimateSize(value: Long) = 8

    override fun write(value: Long, buffer: ByteBufferL) {
        buffer.i64 = value
    }

    override fun read(buffer: ByteBufferL): Long = buffer.i64
}