package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

object CharAdapter : TypeAdapter<Char> {
    override fun estimateSize(value: Char) = 2 // Char is 2 bytes in UTF-16 encoding
    override fun write(value: Char, buffer: ByteBuffer) {
        buffer.putChar(value)
    }

    override fun read(buffer: ByteBuffer) = buffer.char
}