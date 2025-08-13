package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.format.api.RecordReader
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Fixed-length TLV decoder.
 *
 * Record layout:
 *   `[kLen: u16][vLen: u32][seq: u64 LE][flags: u8][key][value]`
 *
 * The buffer's position must point to the start of a record.
 */
object AkkRecordReader : RecordReader {

    private const val HEADER_SIZE = 2 + 4 + 8 + 1

    override fun read(buf: ByteBuffer): Record {
        // Ensure we have at least a header
        require(buf.remaining() >= HEADER_SIZE) { "buffer underflow: need $HEADER_SIZE bytes for header, have ${buf.remaining()}" }

        buf.order(ByteOrder.LITTLE_ENDIAN)

        // ---- header ----
        val kLen = (buf.short.toInt() and 0xFFFF)
        val vLen = buf.int
        val seq = buf.long
        val flags = buf.get()

        require(kLen >= 0) { "negative kLen $kLen" }
        require(vLen >= 0) { "negative vLen $vLen" }

        // ---- key slice ----
        require(buf.remaining() >= kLen) { "truncated key: need $kLen, have ${buf.remaining()}" }
        val keySlice = buf.slice().apply { limit(kLen) }.asReadOnlyBuffer()
        buf.position(buf.position() + kLen)

        // ---- value slice ----
        require(buf.remaining() >= vLen) { "truncated value: need $vLen, have ${buf.remaining()}" }
        val valueSlice = buf.slice().apply { limit(vLen) }.asReadOnlyBuffer()
        buf.position(buf.position() + vLen)

        return Record(keySlice, valueSlice, seq, flags = flags)
    }
}
