package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import dev.swiftstorm.akkaradb.format.RecordReader
import java.nio.ByteBuffer

/**
 * Decodes `[kLen][vLen][seq][key][value]` from a ByteBuffer.
 * The buffer's position must point to the start of a record.
 */
object AkkRecordReader : RecordReader {

    override fun read(buf: ByteBuffer): Record {
        /* ----------- header ----------- */
        val kLen = VarIntCodec.readInt(buf)
        val vLen = VarIntCodec.readInt(buf)
        val seq = VarIntCodec.zigZagDecodeLong(VarIntCodec.readLong(buf))

        /* ----------- key slice ----------- */
        val keySlice = buf.slice().apply {
            limit(kLen)               // slice: position=0, limit=kLen
        }.asReadOnlyBuffer()
        buf.position(buf.position() + kLen)   // advance parent buffer

        /* ----------- value slice ----------- */
        val valueSlice = buf.slice().apply {
            limit(vLen)
        }.asReadOnlyBuffer()
        buf.position(buf.position() + vLen)

        return Record(keySlice, valueSlice, seq)
    }
}