package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.engine.IndexBlock
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

    private val index = IndexBlock()
    private lateinit var bloom: BloomFilter

    fun write(records: List<Record>) {
        require(records.isNotEmpty()) { "records must not be empty" }
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
        index.writeTo(ch)

        val bloomOff = ch.position()
        bloom.writeTo(ch)

        writeFooter(indexOff, bloomOff)
        ch.force(true)
    }

    /* ───────── internal ───────── */

    private fun flushBlock() {
        blockBuf.flip()
        val offset = ch.position()

        // 1) payload length + data
        pool.borrow(4) { lenBuf ->
            lenBuf.clear().putInt(blockBuf.remaining()).flip()
            ch.write(lenBuf)
        }
        ch.write(blockBuf.duplicate())

        // 2) CRC32 of payload
        crc32.reset()
        crc32.update(blockBuf.duplicate())
        pool.borrow(4) { crcBuf ->
            crcBuf.clear().putInt(crc32.value.toInt()).flip()
            ch.write(crcBuf)
        }

        // 3) index entry (only first record)
        val firstKey = AkkRecordReader.read(blockBuf.duplicate()).key.asReadOnlyBuffer()
        index.add(firstKey, offset)

        blockBuf.clear()
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) {
        pool.borrow(20) { ftr ->
            ftr.clear()
            ftr.putInt(0x414B5353)   // "AKSS"
            ftr.putLong(indexOff)
            ftr.putLong(bloomOff)
            ftr.flip()
            ch.write(ftr)
        }
    }

    private fun encode(rec: Record): ByteBuffer =
        pool.get(AkkRecordWriter.computeMaxSize(rec)).also {
            AkkRecordWriter.write(rec, it); it.flip()
        }

    override fun close() {
        pool.release(blockBuf)
        ch.close()
    }
}
