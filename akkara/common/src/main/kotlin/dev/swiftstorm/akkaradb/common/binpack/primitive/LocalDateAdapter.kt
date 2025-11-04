package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.time.LocalDate

object LocalDateAdapter : TypeAdapter<LocalDate> {
    override fun estimateSize(value: LocalDate) = 8

    override fun write(value: LocalDate, buffer: ByteBufferL) {
        buffer.i64 = value.toEpochDay()
    }

    override fun read(buffer: ByteBufferL): LocalDate =
        LocalDate.ofEpochDay(buffer.i64)
}