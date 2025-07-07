package dev.swiftstorm.akkaradb.common.codec

import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * Variable-length integer codec (VarInt / VarLong) with zig-zag helpers.
 * Compatible with Google Protocol Buffers.
 *
 * Little-endian, 7-bit continuation format:
 *   [b6…b0 | c=1] [b13…b7 | c=1] … [bN… | c=0]
 *
 * For signed numbers use Zig-Zag:
 *   zz = (v shl 1) xor (v shr (bitWidth-1))
 */
object VarIntCodec {

    /* ──────────────── write ──────────────── */

    /** Encodes an Int; returns the number of bytes written. */
    fun writeInt(buf: ByteBuffer, value: Int): Int {
        val startPos = buf.position()
        require(buf.remaining() >= 5) { "ByteBuffer too small (need ≤5 bytes)" }

        var v = value
        while (v and 0x7F.inv() != 0) {
            buf.put((v or 0x80).toByte())
            v = v ushr 7
        }
        buf.put(v.toByte())
        return buf.position() - startPos
    }

    /** Encodes a Long; returns the number of bytes written. */
    fun writeLong(buf: ByteBuffer, value: Long): Int {
        val startPos = buf.position()
        require(buf.remaining() >= 10) { "ByteBuffer too small (need ≤10 bytes)" }

        var v = value
        while (v and 0x7FL.inv() != 0L) {
            buf.put((v or 0x80).toByte())
            v = v ushr 7
        }
        buf.put(v.toByte())
        return buf.position() - startPos
    }

    /* ──────────────── read ──────────────── */

    /** Decodes an Int; returns the value. Buffer position advances. */
    @Throws(IllegalArgumentException::class)
    fun readInt(buf: ByteBuffer): Int {
        var shift = 0
        var result = 0
        var b: Int
        try {
            do {
                b = buf.get().toInt() and 0xFF
                result = result or ((b and 0x7F) shl shift)
                shift += 7
                if (shift > 28) error("VarInt too long (overflow)")   // 5 bytes max
            } while (b and 0x80 != 0)
        } catch (e: BufferUnderflowException) {
            throw IllegalArgumentException("VarInt truncated", e)
        }
        return result
    }

    /** Decodes a Long; returns the value. */
    @Throws(IllegalArgumentException::class)
    fun readLong(buf: ByteBuffer): Long {
        var shift = 0
        var result = 0L
        var b: Int
        try {
            do {
                b = buf.get().toInt() and 0xFF
                result = result or ((b and 0x7F).toLong() shl shift)
                shift += 7
                if (shift > 63) error("VarLong too long (overflow)")  // 10 bytes max
            } while (b and 0x80 != 0)
        } catch (e: BufferUnderflowException) {
            throw IllegalArgumentException("VarLong truncated", e)
        }
        return result
    }

    /* ── utility: read & length ── */

    /** Reads an Int **and** returns the decoded value *and* length (bytes consumed). */
    fun readIntWithSize(buf: ByteBuffer): Pair<Int, Int> {
        val pos0 = buf.position()
        val v = readInt(buf)
        return v to (buf.position() - pos0)
    }

    /** Reads a Long **and** returns value + length. */
    fun readLongWithSize(buf: ByteBuffer): Pair<Long, Int> {
        val pos0 = buf.position()
        val v = readLong(buf)
        return v to (buf.position() - pos0)
    }

    /** Returns encoded length (bytes) for an Int without mutating the buffer. */
    fun encodedSize(v: Int): Int =
        max(1, 32 - Integer.numberOfLeadingZeros(v xor (v ushr 31)) + 6) / 7

    /** Same for Long (≤10). */
    fun encodedSize(v: Long): Int =
        max(1, 64 - java.lang.Long.numberOfLeadingZeros(v xor (v ushr 63)) + 6) / 7

    /* ──────────────── zig-zag ──────────────── */

    @JvmStatic
    fun zigZagEncodeInt(v: Int): Int = (v shl 1) xor (v shr 31)
    @JvmStatic
    fun zigZagDecodeInt(v: Int): Int = (v ushr 1) xor -(v and 1)

    @JvmStatic
    fun zigZagEncodeLong(v: Long): Long = (v shl 1) xor (v shr 63)
    @JvmStatic
    fun zigZagDecodeLong(v: Long): Long = (v ushr 1) xor -(v and 1)
}
