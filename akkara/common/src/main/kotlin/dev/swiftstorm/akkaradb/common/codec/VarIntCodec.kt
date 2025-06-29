package dev.swiftstorm.akkaradb.common.codec

import java.nio.BufferUnderflowException
import java.nio.ByteBuffer

/**
 * Utility for variable-length integer (VarInt / VarLong)
 * compatible with Google Protocol Buffers zig-zag encoding.
 *
 * Encoding rule (little-endian, 7 bits per byte):
 *
 *  ┌───────────────┐
 *  │b6 … b0 | c=1  │  ← continuation bit
 *  ├───────────────┤
 *  │b13… b7 | c=1  │
 *  ├───────────────┤
 *  │b20…b14 | c=1  │
 *  ├───────────────┤
 *  │b27…b21 | c=1  │
 *  ├───────────────┤
 *  │b34…b28 | c=1  │  …repeat…
 *  └───────────────┘
 *
 * For signed values use zig-zag transform:
 *   zz = (v shl 1) xor (v shr (bitWidth-1))
 */
object VarIntCodec {

    /* ---------- write ---------- */

    fun writeInt(buf: ByteBuffer, value: Int) {
        var v = value
        while (v and 0x7F.inv() != 0) {
            buf.put((v or 0x80).toByte())
            v = v ushr 7
        }
        buf.put(v.toByte())
    }

    fun writeLong(buf: ByteBuffer, value: Long) {
        var v = value
        while (v and 0x7FL.inv() != 0L) {
            buf.put((v or 0x80).toByte())
            v = v ushr 7
        }
        buf.put(v.toByte())
    }

    /* ---------- read (returns decoded value) ---------- */

    fun readInt(buf: ByteBuffer): Int {
        var shift = 0
        var result = 0
        var b: Int
        try {
            do {
                b = buf.get().toInt() and 0xFF
                result = result or ((b and 0x7F) shl shift)
                shift += 7
                if (shift > 35) throw IllegalArgumentException("VarInt too long (overflow)")
            } while (b and 0x80 != 0)
        } catch (ex: BufferUnderflowException) {
            throw IllegalArgumentException("VarInt truncated or buffer underflow", ex)
        }
        return result
    }

    fun readLong(buf: ByteBuffer): Long {
        var shift = 0
        var result = 0L
        var b: Int
        try {
            do {
                b = buf.get().toInt() and 0xFF
                result = result or ((b and 0x7F).toLong() shl shift)
                shift += 7
                if (shift > 70) throw IllegalArgumentException("VarLong too long (overflow)")
            } while (b and 0x80 != 0)
        } catch (ex: BufferUnderflowException) {
            throw IllegalArgumentException("VarLong truncated or buffer underflow", ex)
        }
        return result
    }

    /* ---------- zig-zag helpers ---------- */

    fun zigZagEncodeInt(v: Int): Int = (v shl 1) xor (v shr 31)
    fun zigZagDecodeInt(v: Int): Int = (v ushr 1) xor -(v and 1)

    fun zigZagEncodeLong(v: Long): Long = (v shl 1) xor (v shr 63)
    fun zigZagDecodeLong(v: Long): Long = (v ushr 1) xor -(v and 1)
}
