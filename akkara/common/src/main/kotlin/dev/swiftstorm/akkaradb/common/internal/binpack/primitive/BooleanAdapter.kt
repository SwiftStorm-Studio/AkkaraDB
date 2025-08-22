package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object BooleanAdapter : TypeAdapter<Boolean> {
    override fun estimateSize(value: Boolean) = 1
    override fun write(value: Boolean, buffer: ByteBuffer) {
        buffer.put(if (value) 1 else 0)
    }

    override fun read(buffer: ByteBuffer) = buffer.get().toInt() != 0
}