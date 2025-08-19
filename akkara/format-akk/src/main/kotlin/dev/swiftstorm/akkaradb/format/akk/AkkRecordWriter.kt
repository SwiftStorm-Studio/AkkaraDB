package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * fixed‑length TLV encoder
 *
 * Layout per record:
 *   `[kLen: u16][vLen: u32][seq: u64 LE][flags: u8][key][value]`
 *
 * Notes
 *  - All multi‑byte fields are **little‑endian**.
 *  - `flags` is a raw u8 as stored in [Record.flags].
 *  - The caller must ensure `dest` has enough remaining space.
 */
object AkkRecordWriter {
    // Header layout: [u16 keyLen][u32 valueLen][u64 seqNo][u8 flags]
    private const val HEADER_SIZE = 2 + 4 + 8 + 1

    /**
     * Encode a record into the destination buffer.
     * The record’s key/value buffers are duplicated as read-only views,
     * so original buffer positions remain untouched.
     *
     * @param record source Record
     * @param dest destination buffer (must have enough remaining space)
     */
    fun write(record: Record, dest: ByteBuffer) {
        val kLen = record.key.remaining()
        val vLen = record.value.remaining()

        require(kLen <= 0xFFFF) { "Key too long ($kLen > 65535)" }
        require(vLen >= 0) { "Negative value length" }
        require(dest.remaining() >= HEADER_SIZE + kLen + vLen) {
            "Destination too small for record"
        }

        val leDest = dest.duplicate().order(ByteOrder.LITTLE_ENDIAN)

        // Write header
        leDest.putShort(kLen.toShort())
        leDest.putInt(vLen)
        leDest.putLong(record.seqNo)  // Note: raw bits preserved, signed Long
        leDest.put(record.flags.toByte())

        // Copy payloads
        val keyBuf = record.key.asReadOnlyBuffer().rewind()
        val valBuf = record.value.asReadOnlyBuffer().rewind()

        leDest.put(keyBuf)
        leDest.put(valBuf)

        // Advance original dest as well
        dest.position(dest.position() + HEADER_SIZE + kLen + vLen)
    }

    /**
     * Compute the maximum buffer size required to store this record.
     */
    fun computeMaxSize(record: Record): Int {
        return HEADER_SIZE + record.key.remaining() + record.value.remaining()
    }
}
