package dev.swiftstorm.akkaradb.common

import java.io.InputStream
import java.io.OutputStream

object VarInt {
    fun write(value: Int, out: OutputStream) {
        var v = value
        while (true) {
            val b = (v and 0x7F)
            v = v ushr 7
            if (v == 0) {
                out.write(b)
                return
            }
            out.write(b or 0x80)
        }
    }

    fun read(`in`: InputStream): Int {
        var shift = 0
        var result = 0
        while (shift < 32) {
            val b = `in`.read()
            require(b != -1) { "EOF while reading VarInt" }
            result = result or ((b and 0x7F) shl shift)
            if (b and 0x80 == 0) return result
            shift += 7
        }
        error("VarInt too long")
    }

    fun size(value: Int): Int =
        if (value and (0.inv() shl 7) == 0) 1
        else if (value and (0.inv() shl 14) == 0) 2
        else if (value and (0.inv() shl 21) == 0) 3
        else if (value and (0.inv() shl 28) == 0) 4
        else 5
}
