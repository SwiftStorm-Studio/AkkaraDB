package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object ByteArrayAdapter : TypeAdapter<ByteArray> {
    override fun estimateSize(value: ByteArray) = 4 + value.size

    override fun write(value: ByteArray, buffer: ByteBuffer) {
        buffer.putInt(value.size)
        buffer.put(value)
    }

    override fun read(buffer: ByteBuffer): ByteArray {
        val size = buffer.int
        val out = ByteArray(size)
        buffer.get(out)
        return out
    }
}