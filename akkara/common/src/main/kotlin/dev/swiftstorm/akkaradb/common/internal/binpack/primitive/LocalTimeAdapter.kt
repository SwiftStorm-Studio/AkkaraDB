package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer
import java.time.LocalTime

object LocalTimeAdapter : TypeAdapter<LocalTime> {
    override fun estimateSize(value: LocalTime) = 8 // Store as nano of day
    override fun write(value: LocalTime, buffer: ByteBuffer) {
        buffer.putLong(value.toNanoOfDay())
    }

    override fun read(buffer: ByteBuffer): LocalTime {
        return LocalTime.ofNanoOfDay(buffer.long)
    }
}