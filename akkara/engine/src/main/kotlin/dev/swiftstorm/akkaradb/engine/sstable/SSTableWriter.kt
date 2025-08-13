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
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.zip.CRC32C

/**
 * SSTable writer (fixed-length encodings to match the reader):
 *  - Block payload = [MiniIndex][Concatenated Records]
 *  - MiniIndex     = [count:u16][offset:u32]×count   (LE)
 *  - Block on disk = [len:u32 BE][ payload ][crc:u32 BE]
 *  - Outer index   = repeat { [keyLen:u16][key][blockOff:i64] } (LE)
 *  - Footer        = ["AKSS":u32][indexOff:u64][bloomOff:u64]
 */
class SSTableWriter(
    private val path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    private val ch = FileChannel.open(path, CREATE, WRITE, TRUNCATE_EXISTING, DSYNC)
    private val blockBuf = pool.get(BLOCK_SIZE)
    private val crc32 = CRC32C()

    private val index = IndexBlock()

    /* ───────── public ───────── */

    fun write(records: Collection<Record>) {
        require(records.isNotEmpty()) { "records must not be empty" }

        // Bloom filter: build alongside writing
        val bloomBuilder = BloomFilter.Builder(records.size)

        var firstKeyInBlock: ByteBuffer? = null
        for (rec in records) {
            val encoded = encode(rec)

            if (blockBuf.remaining() < encoded.remaining()) {
                // flush current block
                blockBuf.flip()
                flushBlock(firstKeyInBlock!!)
                blockBuf.clear()
                firstKeyInBlock = null
            }
            if (firstKeyInBlock == null) firstKeyInBlock = rec.key

            blockBuf.put(encoded.duplicate())
            bloomBuilder.add(rec.key)
            pool.release(encoded)
        }

        val bloom = bloomBuilder.build()

        if (blockBuf.position() > 0) {
            blockBuf.flip()
            flushBlock(firstKeyInBlock!!)
        }

        /* ---- append index + bloom + footer ---- */
        val indexOff = ch.position()
        index.writeTo(ch)
        val bloomOff = ch.position()

        bloom.writeTo(ch)
        val hashCountBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN)
            .putInt(bloom.hashCount).flip() as ByteBuffer
        ch.write(hashCountBuf)

        writeFooter(indexOff, bloomOff)
        ch.force(true)
    }

    /* ───────── internal ───────── */

    private fun flushBlock(firstKey: ByteBuffer) {
        // Build mini-index for the *records area* (blockBuf as it currently stands)
        val miniIdx = buildMiniIndex(blockBuf.duplicate())
        val payloadSize = miniIdx.remaining() + blockBuf.remaining()
        val payload = pool.get(payloadSize)
        payload.put(miniIdx).put(blockBuf).flip()

        val offset = ch.position()

        // Write [len:u32 BE][payload][crc:u32 BE]
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

    /**
     * Build [count:u16][offset:u32]×count for the concatenated records in `data`.
     * Offsets are relative to the start of the records region (i.e., to be
     * added to `dataStart = 2 + 4*count` by the reader).
     */
    private fun buildMiniIndex(data: ByteBuffer): ByteBuffer {
        data.order(ByteOrder.LITTLE_ENDIAN)
        val offsets = ArrayList<Int>(128)
        while (data.hasRemaining()) {
            val start = data.position()
            offsets += start
            // advance to next record using the same decoder as the reader
            AkkRecordReader.read(data)
            // safety: ensure forward progress
            if (data.position() <= start) error("mini-index stalled at $start")
        }

        val count = offsets.size
        val buf = ByteBuffer.allocate(2 + 4 * count).order(ByteOrder.LITTLE_ENDIAN)
        buf.putShort(count.toShort())
        for (off in offsets) buf.putInt(off)
        return buf.flip() as ByteBuffer
    }

    private fun writeFooter(indexOff: Long, bloomOff: Long) =
        pool.borrow(20) { f ->
            f.clear()
            f.order(ByteOrder.BIG_ENDIAN)
            f.putInt(0x414B5353)     // "AKSS"
            f.order(ByteOrder.LITTLE_ENDIAN)
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
