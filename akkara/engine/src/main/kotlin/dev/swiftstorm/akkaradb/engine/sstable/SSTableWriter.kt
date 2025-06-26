package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.zip.CRC32

class SSTableWriter(
    private val path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    private val ch = FileChannel.open(path, CREATE, WRITE, DSYNC)
    private val blockBuf = pool.get(BLOCK_SIZE)
    private val crc32 = CRC32()

    private val index = ArrayList<Pair<ByteBuffer, Long>>()   // firstKey, offset
    private lateinit var bloom: BloomFilter

    fun write(records: List<Record>) {
        require(records.isNotEmpty())
        bloom = BloomFilter(records.size)

        for (rec in records) {
            val encoded = encode(rec)
            if (blockBuf.remaining() < encoded.remaining()) flushBlock()
            blockBuf.put(encoded)
            bloom.add(rec.key)
            pool.release(encoded)
        }
        if (blockBuf.position() > 0) flushBlock()

        val indexOff = ch.position()
        writeIndex()

        val bloomOff = ch.position()
        bloom.writeTo(ch)

        writeFooter(indexOff, bloomOff)
        ch.force(true)
    }

    private fun flushBlock() {
        blockBuf.flip()
        val offset = ch.position()

        ch.write(blockBuf.duplicate())

        crc32.reset()
        crc32.update(blockBuf.duplicate())
        pool.borrow(4) { crcBuf ->
            crcBuf.clear()
            crcBuf.putInt(crc32.value.toInt())
            crcBuf.flip()
            ch.write(crcBuf)
        }

        val firstKey = AkkRecordReader.read(blockBuf.duplicate()).key.asReadOnlyBuffer()
        index += firstKey to offset

        blockBuf.clear()
    }

    private fun writeIndex() {
        val est = index.sumOf { (k, _) -> k.remaining() + 10 }
        var tmp = pool.get(est.coerceAtLeast(1024))
        try {
            index.forEach { (firstKey, off) ->
                VarIntCodec.writeInt(tmp, firstKey.remaining())
                tmp.put(firstKey.duplicate())
                tmp.putLong(off)
            }
            tmp.flip(); ch.write(tmp)
        } finally {
            pool.release(tmp)
        }
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) {
        pool.borrow(20) { ftr ->
            ftr.clear()
            ftr.putInt(0x414B5353)   // "AKSS" magic
            ftr.putLong(indexOff)
            ftr.putLong(bloomOff)
            ftr.flip()
            ch.write(ftr)
        }
    }

    private fun encode(rec: Record): ByteBuffer {
        val buf = pool.get(AkkRecordWriter.computeMaxSize(rec))
        AkkRecordWriter.write(rec, buf); buf.flip()
        return buf
    }

    override fun close() {
        pool.release(blockBuf)
        ch.close()
    }
}
