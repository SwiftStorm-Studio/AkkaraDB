package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.engine.IndexBlock
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.zip.CRC32

/**
 * Immutable single-file SSTable reader.
 *
 *  data blocks ┐
 *  index block │
 *  bloom bits  │
 *  footer 20B  ┘  (magic "AKSS", indexOff, bloomOff)
 */
class SSTableReader(
    path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    /* ───────── file handles ───────── */

    private val ch: FileChannel = FileChannel.open(path, READ)
    private val fileSize: Long = ch.size()

    /* ───────── table meta (footer) ───────── */

    private val indexOff: Long
    private val bloomOff: Long

    /* ───────── in-memory helpers ───────── */

    private val bloom: BloomFilter
    private val index: IndexBlock
    private val crc32 = CRC32()

    init {
        /* ---- 1) footer ---- */
        require(fileSize >= 20) { "file too small: $path" }
        val (idxTmp, bloomTmp) = Pools.io().borrow(20) { f ->
            ch.read(f, fileSize - 20); f.flip()
            require(f.int == 0x414B5353) { "bad footer magic" }
            f.long to f.long
        }
        indexOff = idxTmp
        bloomOff = bloomTmp

        /* ---- 2) bloom bits (mmap) ---- */
        val bloomSize = (fileSize - 20 - bloomOff).toInt()
        bloom = BloomFilter.readFrom(
            ch.map(FileChannel.MapMode.READ_ONLY, bloomOff, bloomSize.toLong())
        )

        /* ---- 3) index block (mmap) ---- */
        val indexSize = (bloomOff - indexOff).toInt()
        index = IndexBlock.readFrom(
            ch.map(FileChannel.MapMode.READ_ONLY, indexOff, indexSize.toLong()),
            indexSize
        )
    }

    /* ───────── public API ───────── */

    /** Bloom filter only – cheap. */
    fun mightContain(key: ByteBuffer): Boolean = bloom.mightContain(key)

    /** Full lookup: bloom → index → block scan → record decode. */
    fun get(key: ByteBuffer): Record? {
        if (!bloom.mightContain(key)) return null          // definite miss

        val off = index.lookup(key)
        if (off < 0) return null                           // range miss

        /* ---- 4) read block header (len) ---- */
        val len = pool.borrow(4) { hdr ->
            hdr.clear(); ch.read(hdr, off); hdr.flip(); hdr.int
        }
        require(len in 0..BLOCK_SIZE) { "payload length=$len out of range" }

        /* ---- 5) read payload + crc ---- */
        val total = 4 + len + 4                            // len + data + crc
        val buf = pool.get(total)
        ch.read(buf, off); buf.flip()

        val payloadLen = buf.int                           // sanity = len
        require(payloadLen == len)

        val payload = buf.slice().apply { limit(payloadLen) }
        buf.position(buf.position() + payloadLen)
        val storedCrc = buf.int

        crc32.reset(); crc32.update(payload.duplicate())
        require(storedCrc == crc32.value.toInt()) { "CRC32 mismatch @$off" }

        /* ---- 6) linear scan inside block ---- */
        while (payload.hasRemaining()) {
            val rec = AkkRecordReader.read(payload)
            if (rec.key.compareTo(key) == 0) {
                pool.release(buf)
                return rec
            }
        }
        pool.release(buf)
        return null
    }

    override fun close() = ch.close()
}
