@file:JvmName("VarInt")

package dev.swiftstorm.akkaradb.common

import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer

/**
 * Utility for encoding and decoding unsigned VarInt values.
 */
object VarInt {
    /** Writes [value] as an unsigned VarInt into the given [OutputStream]. */
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

    /** Reads an unsigned VarInt from the given [InputStream]. */
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

    /** Estimates the size of [value] when encoded as VarInt (performance-oriented). */
    fun size(value: Int): Int =
        if (value and (0.inv() shl 7) == 0) 1
        else if (value and (0.inv() shl 14) == 0) 2
        else if (value and (0.inv() shl 21) == 0) 3
        else if (value and (0.inv() shl 28) == 0) 4
        else 5

    /** Calculates the number of bytes needed to encode [value] as VarInt. */
    fun sizeOfVarInt(value: Int): Int {
        var v = value
        var size = 0
        do {
            size++
            v = v ushr 7
        } while (v != 0)
        return size
    }
}

/** Writes [value] as an unsigned VarInt into this buffer and returns the byte count written. */
fun ByteBuffer.putVarInt(value: Int): Int {
    var v = value
    var written = 0
    while (true) {
        var b = v and 0x7F
        v = v ushr 7
        if (v != 0) b = b or 0x80
        this.put(b.toByte())
        written++
        if (v == 0) break
    }
    return written
}

/** Reads an unsigned VarInt from this buffer. */
fun ByteBuffer.getVarInt(): Int {
    var shift = 0
    var result = 0
    while (true) {
        val b = this.get().toInt() and 0xFF
        result = result or ((b and 0x7F) shl shift)
        if ((b and 0x80) == 0) break
        shift += 7
        require(shift < 32) { "VarInt too long" }
    }
    return result
}