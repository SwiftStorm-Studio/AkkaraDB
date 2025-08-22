package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer
import java.util.*

object DateAdapter : TypeAdapter<Date> {
    override fun estimateSize(value: Date) = 8 // Size of a long in bytes
    override fun write(value: Date, buffer: ByteBuffer) {
        buffer.putLong(value.time)
    }

    override fun read(buffer: ByteBuffer): Date {
        return Date(buffer.long)
    }
}