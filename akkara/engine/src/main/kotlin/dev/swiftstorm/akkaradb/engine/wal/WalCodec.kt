package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import java.nio.ByteBuffer
import java.util.zip.CRC32

object WalCodec {
    fun write(rec: Record, out: ByteBuffer) {
        val lenPos = out.position()
        out.putInt(0)                               // len 仮置き

        val payloadStart = out.position()
        AkkRecordWriter.write(rec, out)
        val payloadLen = out.position() - payloadStart
        out.putInt(lenPos, payloadLen)              // len 確定

        val crc = CRC32().apply {
            update(out.array(), lenPos, 4 + payloadLen)
        }
        out.putInt(crc.value.toInt())
    }

    fun read(src: ByteBuffer, consumer: (Record) -> Unit): Int {
        if (src.remaining() < 8) return 0
        val len = src.int
        if (src.remaining() < len + 4) return 0

        val payload = ByteArray(len)
        src.get(payload)
        val storedCrc = src.int

        val crc = CRC32().apply {
            update(ByteBuffer.allocate(4).putInt(len).array())
            update(payload)
        }.value.toInt()
        require(crc == storedCrc) { "WAL CRC mismatch" }

        val buf = ByteBuffer.wrap(payload)
        consumer(AkkRecordReader.read(buf))
        return 4 + len + 4
    }
}
