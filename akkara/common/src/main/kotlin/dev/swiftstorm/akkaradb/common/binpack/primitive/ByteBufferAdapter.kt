package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import java.nio.ByteBuffer

object ByteBufferAdapter : TypeAdapter<ByteBuffer> {
    override fun estimateSize(value: ByteBuffer): Int = 4 + value.remaining()

    override fun write(value: ByteBuffer, buffer: ByteBufferL) {
        val size = value.remaining()
        buffer.i32 = size
        val dup = value.duplicate()
        if (dup.hasArray()) {
            val arr = dup.array()
            val offset = dup.arrayOffset() + dup.position()
            buffer.putBytes(arr, offset, size)
        } else {
            val tmp = ByteArray(size)
            dup.get(tmp)
            dup.position(dup.position() - size)
            buffer.putBytes(tmp)
        }
    }

    override fun read(buffer: ByteBufferL): ByteBuffer {
        val size = buffer.i32
        require(size >= 0) { "Invalid ByteBuffer length: $size" }
        val slice = buffer.sliceAt(buffer.position, size).rawBuffer()
        buffer.position += size
        return slice
    }
}