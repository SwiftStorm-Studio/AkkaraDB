package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import dev.swiftstorm.akkaradb.format.RecordWriter
import java.nio.ByteBuffer

/**
 * Default TLV encoder used by the “akk” on-disk format:
 *
 *   `[kLen][vLen][seqNo][key][value]`
 *
 * * `kLen` / `vLen`  – VarInt  (1–5 B)
 * * `seqNo`          – VarLong (1–10 B, zig-zag encoded)
 * * `key` / `value`  – raw bytes, no padding
 *
 * Total overhead per record = 3–19 B.
 */
object AkkRecordWriter : RecordWriter {

    /* ---------- public API ---------- */

    override fun write(record: Record, dest: ByteBuffer): Int {
        val startPos = dest.position()

        // Header
        VarIntCodec.writeInt(dest, record.key.size)
        VarIntCodec.writeInt(dest, record.value.size)
        VarIntCodec.writeLong(dest, VarIntCodec.zigZagEncodeLong(record.seqNo))

        // Payload
        dest.put(record.key)
        dest.put(record.value)

        return dest.position() - startPos
    }

    override fun computeMaxSize(record: Record): Int =
        /* worst-case VarInt/VarLong sizes */
        5 + 5 + 10 + record.key.size + record.value.size
}
