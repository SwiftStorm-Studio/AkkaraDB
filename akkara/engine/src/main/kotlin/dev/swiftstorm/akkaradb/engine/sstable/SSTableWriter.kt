package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import dev.swiftstorm.akkaradb.engine.IndexBlock
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.zip.CRC32C

class SSTableWriter(
    private val path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    private val ch = FileChannel.open(path, CREATE, WRITE, DSYNC)
    private val blockBuf = pool.get(BLOCK_SIZE)
    private val crc32 = CRC32C()

    private val index = IndexBlock()
    private lateinit var bloom: BloomFilter

    /* ---------- public ---------- */

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

    /* ---------- internal ---------- */

    private fun flushBlock() {
        blockBuf.flip()

        val firstRecDup = blockBuf.duplicate()
        val miniIdx = buildMiniIndex(blockBuf.duplicate())   // heap buffer

        val payload = pool.get(miniIdx.remaining() + blockBuf.remaining())
        payload.put(miniIdx).put(blockBuf).flip()

        val offset = ch.position()

        val lenBuf = ByteBuffer.allocate(Integer.BYTES).putInt(payload.remaining()).flip() as ByteBuffer
        crc32.reset(); crc32.update(payload.duplicate())
        val crcBuf = ByteBuffer.allocate(Integer.BYTES).putInt(crc32.value.toInt()).flip() as ByteBuffer

        ch.write(arrayOf(lenBuf, payload, crcBuf))

        val firstKey = AkkRecordReader.read(firstRecDup).key.asReadOnlyBuffer()
        index.add(firstKey, offset)

        pool.release(payload)
        blockBuf.clear()
    }

    private fun buildMiniIndex(data: ByteBuffer): ByteBuffer {
        val offsets = ArrayList<Int>(128)
        var prev = 0
        while (data.hasRemaining()) {
            offsets += prev
            AkkRecordReader.read(data)          // advance
            prev = data.position()
        }
        val capacity = offsets.sumOf { VarIntCodec.encodedSize(it) } +
                VarIntCodec.encodedSize(offsets.size)
        val buf = ByteBuffer.allocate(capacity)
        VarIntCodec.writeInt(buf, offsets.size)
        var prevOff = 0
        for (abs in offsets) {
            VarIntCodec.writeInt(buf, abs - prevOff)
            prevOff = abs
        }
        return buf.flip().asReadOnlyBuffer()
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) =
        pool.borrow(20) { footer ->
            footer.clear()
            footer.putInt(0x414B5353)          // "AKSS"
            footer.putLong(indexOff)
            footer.putLong(bloomOff)
            footer.flip(); ch.write(footer)
        }

    private fun encode(rec: Record): ByteBuffer {
        val buf = pool.get(AkkRecordWriter.computeMaxSize(rec))
        AkkRecordWriter.write(rec, buf)
        buf.flip()
        require(buf.remaining() <= BLOCK_SIZE) {
            "Single record (${buf.remaining()} B) exceeds block size ($BLOCK_SIZE B)"
        }
        return buf
    }

    /* ---------- lifecycle ---------- */

    override fun close() {
        pool.release(blockBuf)
        ch.close()
    }
}
