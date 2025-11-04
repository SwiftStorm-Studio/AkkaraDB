package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.util.*

object DateAdapter : TypeAdapter<Date> {
    override fun estimateSize(value: Date) = 8

    override fun write(value: Date, buffer: ByteBufferL) {
        buffer.i64 = value.time
    }

    override fun read(buffer: ByteBufferL): Date {
        return Date(buffer.i64)
    }
}