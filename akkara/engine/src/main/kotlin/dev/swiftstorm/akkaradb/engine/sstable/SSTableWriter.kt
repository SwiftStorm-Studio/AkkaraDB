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
        val firstRecDup = blockBuf.duplicate()

        // 0) Build mini‑index over blockBuf
        val idxTmp = buildMiniIndex(blockBuf.duplicate())     // returns read‑only dup

        // 1) Assemble payload = [miniIndex][data]
        val payload = pool.get(idxTmp.remaining() + blockBuf.remaining())
        payload.put(idxTmp).put(blockBuf).flip()

        val offset = ch.position()            // ← absolute file offset of this block

        // 2) Write [len][payload][crc]
        pool.borrow(4) { lenBuf ->
            lenBuf.clear().putInt(payload.remaining()).flip()
            ch.write(lenBuf)
        }
        ch.write(payload.duplicate())

        crc32.reset(); crc32.update(payload.duplicate())
        pool.borrow(4) { cBuf ->
            cBuf.clear().putInt(crc32.value.toInt()).flip()
            ch.write(cBuf)
        }

        // 3) outer index – first key of the block
        val firstKey = AkkRecordReader.read(firstRecDup).key.asReadOnlyBuffer()
        index.add(firstKey, offset)

        // 4) cleanup
        pool.release(payload)
        blockBuf.clear()
    }

    /** Builds a VarInt‑encoded mini‑index and returns a read‑only buffer. */
    private fun buildMiniIndex(data: ByteBuffer): ByteBuffer {
        val offsets = ArrayList<Int>(128)
        var prev = 0
        while (data.hasRemaining()) {
            offsets += prev
            AkkRecordReader.read(data)          // advance position
            prev = data.position()
        }
        // encode: [count][delta1][delta2]… – delta from previous
        val cap = offsets.sumOf { VarIntCodec.encodedSize(it) } + VarIntCodec.encodedSize(offsets.size)
        val buf = ByteBuffer.allocate(cap)
        VarIntCodec.writeInt(buf, offsets.size)
        var prevOff = 0
        for (abs in offsets) {
            VarIntCodec.writeInt(buf, abs - prevOff)
            prevOff = abs
        }
        return buf.flip().asReadOnlyBuffer()
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) {
        pool.borrow(20) { ftr ->
            ftr.clear()
            ftr.putInt(0x414B5353)   // "AKSS"
            ftr.putLong(indexOff)
            ftr.putLong(bloomOff)
            ftr.flip(); ch.write(ftr)
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