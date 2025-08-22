package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer


class StringAdapter(private val validate: Boolean = true) : TypeAdapter<String> {

    override fun estimateSize(value: String): Int {
        return 4 + value.length
    }

    override fun write(value: String, buffer: ByteBuffer) {
        val len = value.length
        if (buffer.remaining() < 4 + len) throw java.nio.BufferOverflowException()

        val start = buffer.position()
        buffer.position(start + 4)

        var i = 0
        while (i < len) {
            val c = value[i].code  // UTF-16
            if (validate && (c and 0x80) != 0) {
                throw IllegalArgumentException(
                    "Non-ASCII char detected: U+%04X at index %d".format(c, i)
                )
            }
            buffer.put(c.toByte()) // ISO-8859-1
            i++
        }

        buffer.putInt(start, len)
    }

    override fun read(buffer: ByteBuffer): String {
        if (buffer.remaining() < 4) throw java.nio.BufferUnderflowException()
        val size = buffer.int
        if (size == 0) return ""
        require(size >= 0) { "Negative size: $size" }
        val remaining = buffer.remaining()
        if (remaining < size) throw java.nio.BufferUnderflowException()

        val start = buffer.position()
        val end = start + size

        if (validate) {
            var p = start
            while (p < end) {
                val b = buffer.get(p).toInt()
                if ((b and 0x80) != 0) {
                    throw IllegalArgumentException("Non-ASCII byte at offset ${p - start}")
                }
                p++
            }
        }

        val sb = StringBuilder(size)
        var p = start
        if (validate) {
            while (p < end) {
                sb.append((buffer.get(p).toInt() and 0x7F).toChar())
                p++
            }
        } else {
            while (p < end) {
                sb.append((buffer.get(p).toInt() and 0xFF).toChar())
                p++
            }
        }

        buffer.position(end)
        return sb.toString()
    }
}
