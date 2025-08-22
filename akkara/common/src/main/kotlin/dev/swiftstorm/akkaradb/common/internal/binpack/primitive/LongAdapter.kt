package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object LongAdapter : TypeAdapter<Long> {
    override fun estimateSize(value: Long) = 8
    override fun write(value: Long, buffer: ByteBuffer) {
        buffer.putLong(value)
    }

    override fun read(buffer: ByteBuffer) = buffer.long
}