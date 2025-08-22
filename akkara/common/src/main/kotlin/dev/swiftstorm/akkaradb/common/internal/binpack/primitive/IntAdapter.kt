package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object IntAdapter : TypeAdapter<Int> {
    override fun estimateSize(value: Int) = 4
    override fun write(value: Int, buffer: ByteBuffer) {
        buffer.putInt(value)
    }

    override fun read(buffer: ByteBuffer) = buffer.int
}

