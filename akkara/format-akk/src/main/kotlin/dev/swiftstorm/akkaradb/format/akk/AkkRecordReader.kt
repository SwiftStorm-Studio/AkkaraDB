package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
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
object AkkRecordReader {
    // Header layout: [u16 keyLen][u32 valueLen][u64 seqNo][u8 flags]
    private const val HEADER_SIZE = 2 + 4 + 8 + 1

    /**
     * Decode a Record from a buffer.
     * The buffer is not mutated (uses duplicate + slice).
     *
     * @param buf source buffer containing one encoded record
     * @return parsed Record with key/value slices (read-only views)
     */
    fun read(buf: ByteBuffer): Record {
        val leBuf = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN)

        require(leBuf.remaining() >= HEADER_SIZE) {
            "Buffer too small for record header"
        }

        val kLen = leBuf.short.toInt() and 0xFFFF
        val vLen = leBuf.int
        require(kLen >= 0 && vLen >= 0) {
            "Negative length (k=$kLen, v=$vLen)"
        }

        val seqNo = leBuf.long
        val flags = leBuf.get()

        require(leBuf.remaining() >= kLen + vLen) {
            "Buffer too small for record payload (k=$kLen, v=$vLen)"
        }

        // Slice key/value without copying
        val keySlice = leBuf.slice(0, kLen).asReadOnlyBuffer()
        leBuf.position(leBuf.position() + kLen)

        val valSlice = leBuf.slice(0, vLen).asReadOnlyBuffer()

        // Use public constructor (which computes keyHash)
        return Record(keySlice, valSlice, seqNo, flags)
    }
}