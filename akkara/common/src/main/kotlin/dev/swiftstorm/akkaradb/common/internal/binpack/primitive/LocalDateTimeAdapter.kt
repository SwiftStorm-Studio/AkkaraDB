package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.time.ZoneOffset

object LocalDateTimeAdapter : TypeAdapter<LocalDateTime> {
    override fun estimateSize(value: LocalDateTime) = 12 // Store as epoch seconds and nano
    override fun write(value: LocalDateTime, buffer: ByteBuffer) {
        buffer.putLong(value.toEpochSecond(ZoneOffset.UTC))
        buffer.putInt(value.nano)
    }

    override fun read(buffer: ByteBuffer): LocalDateTime {
        val epochSeconds = buffer.long
        val nano = buffer.int
        return LocalDateTime.ofEpochSecond(epochSeconds, nano, ZoneOffset.UTC)
    }
}