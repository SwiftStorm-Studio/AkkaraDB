package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.format.api.RecordWriter
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
object AkkRecordWriter : RecordWriter {

    private const val HEADER_SIZE = 2 + 4 + 8 + 1   // = 15 bytes
    private const val U16_MAX = 0xFFFF

    override fun write(record: Record, dest: ByteBuffer): Int {
        val startPos = dest.position()

        // Create stable, read‑only views of key/value
        val keyBuf = record.key.duplicate().apply { position(0); limit(record.key.remaining()) }.asReadOnlyBuffer()
        val valBuf = record.value.duplicate().apply { position(0); limit(record.value.remaining()) }.asReadOnlyBuffer()

        val kLen = keyBuf.remaining()
        val vLen = valBuf.remaining()

        require(kLen <= U16_MAX) { "key length $kLen exceeds u16 limit ($U16_MAX)" }
        // vLen is u32; ByteBuffer.putInt covers 0..2^31-1 for positive values; negative would wrap.
        // We just guard non‑negative and leave >2GiB to the caller/environment limits.
        require(vLen >= 0) { "value length must be non‑negative" }

        // Ensure capacity (optional but helpful for early error)
        val needed = HEADER_SIZE + kLen + vLen
        require(dest.remaining() >= needed) { "buffer too small: need $needed, have ${dest.remaining()}" }

        // Write header in Little‑Endian
        dest.order(ByteOrder.LITTLE_ENDIAN)
        dest.putShort(kLen.toShort())        // u16
        dest.putInt(vLen)                    // u32 (non‑negative int)
        dest.putLong(record.seqNo)           // u64 LE (signed in JVM, raw bits preserved)
        dest.put(record.flags)               // u8

        // Write payload
        dest.put(keyBuf)
        dest.put(valBuf)

        return dest.position() - startPos
    }

    override fun computeMaxSize(record: Record): Int {
        val k = record.key.remaining()
        val v = record.value.remaining()
        return HEADER_SIZE + k + v
    }
}
