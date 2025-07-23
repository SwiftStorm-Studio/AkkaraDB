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
                blockBuf.flip()
                flushBlock(firstKeyInBlock!!)
                blockBuf.clear()
                firstKeyInBlock = null
            }
            if (firstKeyInBlock == null) firstKeyInBlock = rec.key

            blockBuf.put(encoded.duplicate())
            bloom.add(rec.key)
            pool.release(encoded)
        }
        if (blockBuf.position() > 0) {
            blockBuf.flip()
            flushBlock(firstKeyInBlock!!)
        }

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
        val miniIdx = buildMiniIndex(blockBuf.duplicate())

        val idxSize = miniIdx.remaining()

        val payloadSize = idxSize + blockBuf.remaining()
        val payload = pool.get(payloadSize)

        payload.put(miniIdx).put(blockBuf).flip()

        val offset = ch.position()

        val lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN)
            .putInt(payload.remaining()).flip() as ByteBuffer
        crc32.reset(); crc32.update(payload.duplicate())
        val crcBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN)
            .putInt(crc32.value.toInt()).flip() as ByteBuffer
        ch.write(arrayOf(lenBuf, payload, crcBuf))

        index.add(firstKey.asReadOnlyBuffer(), offset)

        pool.release(payload)
        blockBuf.clear()
    }

    private fun buildMiniIndex(data: ByteBuffer): ByteBuffer {
        val offsets = ArrayList<Int>(128)
        println("Remaining in block: ${data.remaining()} bytes")
        while (data.hasRemaining()) {
            val startPos = data.position()
            try {
                offsets += startPos
                AkkRecordReader.read(data)
            } catch (e: Exception) {
                println("MiniIndex read error: ${e.message} at $startPos, remaining=${data.remaining()}")
                break
            }
            if (data.position() <= startPos) {
                println("MiniIndex: position didn't advance at $startPos, break")
                break
            }
        }


        val buf = ByteBuffer.allocate(5 * (offsets.size + 1))

        VarIntCodec.writeInt(buf, offsets.size)
        var last = 0
        for (abs in offsets) {
            VarIntCodec.writeInt(buf, abs - last)
            last = abs
        }
        return buf.flip()
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
            AkkRecordWriter.write(rec, buf); buf.flip(); buf.slice()
        }

    /* ───────── lifecycle ───────── */

    override fun close() {
        pool.release(blockBuf)
        ch.close()
    }
}
