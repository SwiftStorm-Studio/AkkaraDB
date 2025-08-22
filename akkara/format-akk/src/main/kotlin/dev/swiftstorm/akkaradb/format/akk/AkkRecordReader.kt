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
    fun read(buf: ByteBuffer): Record {
        val le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        val start = le.position()

        val kLen = le.short.toInt() and 0xFFFF
        val vLen = le.int
        val seq = le.long
        val flags = le.get()

        val kPos = le.position()
        val keyRO = run {
            val tmp = le.duplicate()
            tmp.limit(kPos + kLen)
            tmp.position(kPos)
            tmp.slice()
        }
        le.position(kPos + kLen)

        val vPos = le.position()
        val valRO = run {
            val tmp = le.duplicate()
            tmp.limit(vPos + vLen)
            tmp.position(vPos)
            tmp.slice()
        }
        le.position(vPos + vLen)

        val consumed = le.position() - start
        buf.position(buf.position() + consumed)

        return Record(keyRO, valRO, seq, flags)
    }
}
