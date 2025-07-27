package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import dev.swiftstorm.akkaradb.format.api.RecordWriter
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

    override fun write(record: Record, dest: ByteBuffer): Int {
        val startPos = dest.position()

        val keyBuf = record.key.duplicate().apply { position(0); limit(record.key.remaining()) }.asReadOnlyBuffer()
        val valBuf = record.value.duplicate().apply { position(0); limit(record.value.remaining()) }.asReadOnlyBuffer()

        // Header
        VarIntCodec.writeInt(dest, keyBuf.remaining())
        VarIntCodec.writeInt(dest, valBuf.remaining())
        VarIntCodec.writeLong(dest, VarIntCodec.zigZagEncodeLong(record.seqNo))

        // Payload
        dest.put(keyBuf)
        dest.put(valBuf)

        return dest.position() - startPos
    }

    override fun computeMaxSize(record: Record): Int =
        5 + 5 + 10 + record.key.remaining() + record.value.remaining()
}
