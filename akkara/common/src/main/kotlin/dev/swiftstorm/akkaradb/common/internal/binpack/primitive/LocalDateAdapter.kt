package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer
import java.time.LocalDate

object LocalDateAdapter : TypeAdapter<LocalDate> {
    override fun estimateSize(value: LocalDate) = 8 // Size of a long in bytes (to store epoch days)
    override fun write(value: LocalDate, buffer: ByteBuffer) {
        buffer.putLong(value.toEpochDay())
    }

    override fun read(buffer: ByteBuffer): LocalDate {
        return LocalDate.ofEpochDay(buffer.long)
    }
}