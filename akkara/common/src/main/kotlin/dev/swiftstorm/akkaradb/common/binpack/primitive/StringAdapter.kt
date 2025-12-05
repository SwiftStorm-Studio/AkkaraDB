package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterSetting
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

/**
 * ASCII/ISO-8859-1 string adapter.
 *
 * Encoding:
 *   [i32 length][bytes(length)]
 * Constraints:
 *   - When validate=true, only 0x00..0x7F are allowed.
 * Notes:
 *   - Length is written first as a placeholder and patched after writing bytes
 *     to avoid double-pass when computing byte length.
 */
class StringAdapter(private val validate: Boolean = true) : TypeAdapter<String> {

    override fun estimateSize(value: String): Int {
        // ASCII/ISO-8859-1 => 1 byte per char
        return 4 + value.length
    }

    override fun write(value: String, buffer: ByteBufferL) {
        val start = buffer.position
        buffer.i32 = 0 // placeholder for length

        var written = 0
        val len = value.length
        var i = 0
        while (i < len) {
            val c = value[i].code
            if (validate && (c and 0x80) != 0) {
                throw IllegalArgumentException(
                    "Non-ASCII char detected: U+%04X at index %d".format(c, i)
                )
            }
            buffer.i8 = (c and 0xFF)
            written++
            i++
        }

        // Patch the length field
        buffer.putI32At(start, written)
    }

    override fun read(buffer: ByteBufferL): String {
        val size = buffer.i32
        require(size >= 0) { "Negative string size: $size" }
        require(size <= AdapterSetting.maxStringLength) {
            "String size $size exceeds configured limit (${AdapterSetting.maxStringLength})"
        }
        require(buffer.remaining >= size) {
            "Insufficient bytes: need=$size remaining=${buffer.remaining}"
        }

        val sb = StringBuilder(size)
        var k = 0
        if (validate) {
            while (k < size) {
                val b = buffer.i8
                if ((b and 0x80) != 0) {
                    throw IllegalArgumentException("Non-ASCII byte at offset $k")
                }
                sb.append((b and 0x7F).toChar())
                k++
            }
        } else {
            while (k < size) {
                val b = buffer.i8
                sb.append((b and 0xFF).toChar())
                k++
            }
        }
        return sb.toString()
    }
}
