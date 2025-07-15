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
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.zip.CRC32C

class SSTableWriter(
    private val path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    private val ch = FileChannel.open(path, CREATE, WRITE, TRUNCATE_EXISTING, DSYNC)
    private val blockBuf = pool.get(BLOCK_SIZE)
    private val crc32 = CRC32C()

    private val index = IndexBlock()
    private lateinit var bloom: BloomFilter

    /* ───────── public ───────── */

    fun write(records: List<Record>) {
        require(records.isNotEmpty()) { "records must not be empty" }
        bloom = BloomFilter(records.size)

        var firstKeyInBlock: ByteBuffer? = null
        for (rec in records) {
            val encoded = encode(rec)
            if (blockBuf.remaining() < encoded.remaining()) {
                flushBlock(firstKeyInBlock!!)
                firstKeyInBlock = null
            }
            if (firstKeyInBlock == null) firstKeyInBlock = rec.key

            blockBuf.put(encoded)
            bloom.add(rec.key)
            pool.release(encoded)
        }
        if (blockBuf.position() > 0) flushBlock(firstKeyInBlock!!)

        /* ---- append index + bloom + footer ---- */
        val indexOff = ch.position()
        index.writeTo(ch)
        val bloomOff = ch.position()
        bloom.writeTo(ch)
        writeFooter(indexOff, bloomOff)
        ch.force(true)
    }

    /* ───────── internal ───────── */

    private fun flushBlock(firstKey: ByteBuffer) {
        blockBuf.flip()

        // build mini-index (delta varint) + payload concat
        val miniIdx = buildMiniIndex(blockBuf.duplicate())
        val payload = pool.get(miniIdx.remaining() + blockBuf.remaining())
        payload.put(miniIdx).put(blockBuf).flip()

        val offset = ch.position()

        // scatter-gather: [len][payload][crc]
        val lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(payload.remaining()).flip() as ByteBuffer
        crc32.reset(); crc32.update(payload.duplicate())
        val crcBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(crc32.value.toInt()).flip() as ByteBuffer
        ch.write(arrayOf(lenBuf, payload, crcBuf))

        index.add(firstKey.asReadOnlyBuffer(), offset)

        pool.release(payload)
        blockBuf.clear()
    }

    private fun buildMiniIndex(data: ByteBuffer): ByteBuffer {
        val offsets = ArrayList<Int>(128)
        var prev = 0
        while (data.hasRemaining()) {
            offsets += prev
            AkkRecordReader.read(data)
            prev = data.position()
        }
        val capacity = VarIntCodec.encodedSize(offsets.size) +
                offsets.sumOf { VarIntCodec.encodedSize(it) }
        val buf = ByteBuffer.allocate(capacity)
        VarIntCodec.writeInt(buf, offsets.size)
        var last = 0
        for (abs in offsets) {
            VarIntCodec.writeInt(buf, abs - last)
            last = abs
        }
        return buf.flip().asReadOnlyBuffer()
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) =
        pool.borrow(20) { f ->
            f.clear()
            f.putInt(0x414B5353)     // "AKSS"
            f.putLong(indexOff)
            f.putLong(bloomOff)
            f.flip(); ch.write(f)
        }

    private fun encode(rec: Record): ByteBuffer =
        pool.borrow(AkkRecordWriter.computeMaxSize(rec)) { buf ->
            AkkRecordWriter.write(rec, buf); buf.flip(); buf.asReadOnlyBuffer()
        }

    /* ───────── lifecycle ───────── */

    override fun close() {
        pool.release(blockBuf)
        ch.close()
    }
}
