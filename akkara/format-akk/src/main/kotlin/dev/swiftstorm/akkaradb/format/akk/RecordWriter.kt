package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.VarInt
import java.io.ByteArrayOutputStream

object RecordWriter {
    fun toBytes(rec: Record): ByteArray {
        val out = ByteArrayOutputStream(
            VarInt.size(rec.key.size) +
                    VarInt.size(rec.value.size) +
                    VarInt.size(8) +           // seqNo VarInt
                    rec.key.size + rec.value.size
        )
        VarInt.write(rec.key.size, out)
        VarInt.write(rec.value.size, out)
        VarInt.write(rec.seqNo.toInt(), out)
        out.write(rec.key)
        out.write(rec.value)
        return out.toByteArray()
    }
}