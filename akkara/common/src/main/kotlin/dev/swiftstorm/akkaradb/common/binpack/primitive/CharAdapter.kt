package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

/**
 * UTF-16 code unit adapter (2 bytes).
 * Stored as raw 16-bit little-endian value using ByteBufferL.i16.
 * This preserves the exact 16-bit pattern of Kotlin Char.
 */
object CharAdapter : TypeAdapter<Char> {
    override fun estimateSize(value: Char) = 2

    override fun write(value: Char, buffer: ByteBufferL) {
        // Write low 16 bits (UTF-16 code unit) in LE
        buffer.i16 = value.code.toShort()
    }

    override fun read(buffer: ByteBufferL): Char {
        // Read signed short then widen to unsigned 16-bit range
        return (buffer.i16.toInt() and 0xFFFF).toChar()
    }
}