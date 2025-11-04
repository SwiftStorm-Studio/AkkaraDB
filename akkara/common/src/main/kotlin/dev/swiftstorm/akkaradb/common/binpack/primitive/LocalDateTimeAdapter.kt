package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.time.LocalDateTime
import java.time.ZoneOffset

object LocalDateTimeAdapter : TypeAdapter<LocalDateTime> {
    override fun estimateSize(value: LocalDateTime) = 12

    override fun write(value: LocalDateTime, buffer: ByteBufferL) {
        buffer.i64 = value.toEpochSecond(ZoneOffset.UTC)
        buffer.i32 = value.nano
    }

    override fun read(buffer: ByteBufferL): LocalDateTime {
        val epochSeconds = buffer.i64
        val nano = buffer.i32
        return LocalDateTime.ofEpochSecond(epochSeconds, nano, ZoneOffset.UTC)
    }
}