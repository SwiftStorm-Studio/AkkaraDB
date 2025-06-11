@file:Suppress("DuplicatedCode")

package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.VarInt
import java.io.ByteArrayInputStream

object RecordReader {
    fun fromBytes(buf: ByteArray): Record {
        val `in` = ByteArrayInputStream(buf)
        val kLen = VarInt.read(`in`)
        val vLen = VarInt.read(`in`)
        val seq  = VarInt.read(`in`).toLong()
        val key  = `in`.readNBytes(kLen)
        val valB = `in`.readNBytes(vLen)
        return Record(key, valB, seq)
    }

    fun fromBytesOrNull(buf: ByteArray?): Record? {
        if (buf == null) return null
        val `in` = ByteArrayInputStream(buf)
        val kLen = VarInt.read(`in`)
        val vLen = VarInt.read(`in`)
        val seq  = VarInt.read(`in`).toLong()
        val key  = `in`.readNBytes(kLen)
        val valB = `in`.readNBytes(vLen)
        return Record(key, valB, seq)
    }
}
