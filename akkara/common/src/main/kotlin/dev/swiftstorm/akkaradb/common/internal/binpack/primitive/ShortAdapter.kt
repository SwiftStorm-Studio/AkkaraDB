package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object ShortAdapter : TypeAdapter<Short> {
    override fun estimateSize(value: Short) = 2
    override fun write(value: Short, buffer: ByteBuffer) {
        buffer.putShort(value)
    }

    override fun read(buffer: ByteBuffer) = buffer.short
}