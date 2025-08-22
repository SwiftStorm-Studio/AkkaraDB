package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object ByteBufferAdapter : TypeAdapter<ByteBuffer> {
    override fun estimateSize(value: ByteBuffer) = 4 + value.remaining()

    override fun write(value: ByteBuffer, buffer: ByteBuffer) {
        val size = value.remaining()
        buffer.putInt(size)
        buffer.put(value.duplicate())
    }

    override fun read(buffer: ByteBuffer): ByteBuffer {
        val size = buffer.int
        val slice = buffer.slice(buffer.position(), size)
        buffer.position(buffer.position() + size)
        return slice
    }
}
