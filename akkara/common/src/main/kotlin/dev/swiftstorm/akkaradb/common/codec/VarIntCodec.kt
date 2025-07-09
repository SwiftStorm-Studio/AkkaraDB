package dev.swiftstorm.akkaradb.common.codec

import java.nio.BufferUnderflowException
import java.nio.ByteBuffer

/**
 * Variable-length integer codec (VarInt / VarLong) + Zig-Zag helpers.
 */
object VarIntCodec {

    /* ───────── constants ───────── */

    private const val MAX_VARINT_SIZE_INT = 5
    private const val MAX_VARINT_SIZE_LONG = 10

    /* ───────── write ───────── */

    /** Encodes an *unsigned* Int. Returns bytes written. */
    fun writeInt(buf: ByteBuffer, value: Int): Int {
        require(buf.remaining() >= MAX_VARINT_SIZE_INT) { "Need ≤5 bytes, have ${buf.remaining()}" }
        val start = buf.position()

        var v = value
        // fast-path 1 byte
        if (v and 0x7F.inv() == 0) {
            buf.put(v.toByte())
            return 1
        }
        while (v and 0x7F.inv() != 0) {
            buf.put((v or 0x80).toByte())
            v = v ushr 7
        }
        buf.put(v.toByte())
        return buf.position() - start
    }

    /** Encodes an *unsigned* Long. Returns bytes written. */
    fun writeLong(buf: ByteBuffer, value: Long): Int {
        require(buf.remaining() >= MAX_VARINT_SIZE_LONG) { "Need ≤10 bytes, have ${buf.remaining()}" }
        val start = buf.position()

        var v = value
        if (v and 0x7F.inv() == 0L) {
            buf.put(v.toByte())
            return 1
        }
        while (v and 0x7F.inv() != 0L) {
            buf.put((v or 0x80).toByte())
            v = v ushr 7
        }
        buf.put(v.toByte())
        return buf.position() - start
    }

    /* ───────── read ───────── */

    /** Decodes an *unsigned* Int. */
    @Throws(IllegalArgumentException::class)
    fun readInt(buf: ByteBuffer): Int {
        var shift = 0
        var result = 0
        try {
            while (true) {
                val b = buf.get().toInt() and 0xFF
                result = result or ((b and 0x7F) shl shift)

                // 最終バイト (continuation=0)
                if (b and 0x80 == 0) return result

                shift += 7
                if (shift >= 35) error("VarInt too long (overflow)")   // >5 bytes
            }
        } catch (e: BufferUnderflowException) {
            throw IllegalArgumentException("VarInt truncated", e)
        }
    }

    /** Decodes an *unsigned* Long. */
    @Throws(IllegalArgumentException::class)
    fun readLong(buf: ByteBuffer): Long {
        var shift = 0
        var result = 0L
        try {
            while (true) {
                val b = buf.get().toInt() and 0xFF
                result = result or ((b and 0x7F).toLong() shl shift)

                if (b and 0x80 == 0) return result

                shift += 7
                if (shift >= 70) error("VarLong too long (overflow)")  // >10 bytes
            }
        } catch (e: BufferUnderflowException) {
            throw IllegalArgumentException("VarLong truncated", e)
        }
    }

    /* ───────── helpers ───────── */

    /** Reads an Int and also returns consumed byte length. */
    fun readIntWithSize(buf: ByteBuffer): Pair<Int, Int> {
        val pos0 = buf.position()
        val v = readInt(buf)
        return v to (buf.position() - pos0)
    }

    /** Reads a Long and also returns consumed byte length. */
    fun readLongWithSize(buf: ByteBuffer): Pair<Long, Int> {
        val pos0 = buf.position()
        val v = readLong(buf)
        return v to (buf.position() - pos0)
    }

    /** Encoded byte length for a signed Int */
    fun encodedSize(v: Int): Int {
        val u = v xor (v ushr 31)                 // zig-zag to unsigned
        if (u == 0) return 1
        val bits = 32 - Integer.numberOfLeadingZeros(u)
        return (bits + 6) / 7                     // ceil(bits/7)
    }

    /** Encoded byte length for a signed Long. */
    fun encodedSize(v: Long): Int {
        val u = v xor (v ushr 63)
        if (u == 0L) return 1
        val bits = 64 - java.lang.Long.numberOfLeadingZeros(u)
        return (bits + 6) / 7
    }

    /* ───────── zig-zag ───────── */

    @JvmStatic
    fun zigZagEncodeInt(v: Int): Int = (v shl 1) xor (v shr 31)

    @JvmStatic
    fun zigZagDecodeInt(v: Int): Int = (v ushr 1) xor -(v and 1)

    @JvmStatic
    fun zigZagEncodeLong(v: Long): Long = (v shl 1) xor (v shr 63)

    @JvmStatic
    fun zigZagDecodeLong(v: Long): Long = (v ushr 1) xor -(v and 1)
}