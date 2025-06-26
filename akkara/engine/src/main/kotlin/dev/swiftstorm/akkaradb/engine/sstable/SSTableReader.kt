package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.common.codec.VarIntCodec
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

/**
 * One-file SSTable reader.
 *
 *  data blocks ┐
 *  index block │ ← mmap 読み切り
 *  bloom bits  │
 *  footer 20 B ┘ ('AKSS' magic, indexOff, bloomOff)
 */
class SSTableReader(
    private val path: Path,
    private val pool: BufferPool = Pools.io()
) : Closeable {

    /* -------------- disk handles -------------- */

    private val ch: FileChannel = FileChannel.open(path, READ)
    private val fileSize = ch.size()

    /* -------------- footer / table meta -------------- */

    private val indexOff: Long
    private val bloomOff: Long

    /* -------------- in-memory structures -------------- */

    private val bloom: BloomFilter
    private val indexList: List<Pair<ByteBuffer, Long>>

    init {
        // ---- 1) footer ----
        require(fileSize >= 20) { "file too small: $path" }

        val (idxOffTmp, bloomOffTmp) = Pools.io().borrow(20) { buf ->
            ch.read(buf, fileSize - 20); buf.flip()

            require(buf.int == 0x414B5353) { "bad magic in footer: $path" } // 'AKSS'
            buf.long to buf.long
        }
        indexOff  = idxOffTmp
        bloomOff  = bloomOffTmp

        // ---- 2) read bloom ----
        val bloomSize = (fileSize - 20 - bloomOff).toInt()
        bloom = BloomFilter.readFrom(
            ch.map(FileChannel.MapMode.READ_ONLY, bloomOff, bloomSize.toLong()),
            bloomSize
        )

        // ---- 3) read index block ----
        val indexSize = (bloomOff - indexOff).toInt()
        val idxBuf = ch.map(FileChannel.MapMode.READ_ONLY, indexOff, indexSize.toLong())
        indexList = buildIndexList(idxBuf)
    }


    /* -------------- public API -------------- */

    fun mightContain(key: ByteBuffer): Boolean = bloom.mightContain(key)

    fun get(key: ByteBuffer): Record? {
        if (!bloom.mightContain(key)) return null

        val off = binarySearchIndex(key)
        if (off < 0) return null

        // ---- read data block ----
        val blockBuf = pool.get(BLOCK_SIZE + 4)
        blockBuf.clear()
        ch.read(blockBuf, off)
        blockBuf.flip()

        val payloadLen = blockBuf.int
        val payloadBuf = blockBuf.slice().apply { limit(payloadLen) }

        val reader = AkkRecordReader
        while (payloadBuf.hasRemaining()) {
            val rec = reader.read(payloadBuf)
            if (rec.key == key) {
                pool.release(blockBuf)
                return rec
            }
        }
        pool.release(blockBuf)
        return null
    }

    override fun close() = ch.close()

    /* -------------- helpers -------------- */

    private fun buildIndexList(buf: ByteBuffer): List<Pair<ByteBuffer, Long>> {
        val list = ArrayList<Pair<ByteBuffer, Long>>()
        while (buf.hasRemaining()) {
            val len = VarIntCodec.readInt(buf)
            val keySlice = buf.slice().apply {
                limit(len)
            }.asReadOnlyBuffer()
            buf.position(buf.position() + len)
            val off = buf.long
            list += keySlice to off
        }
        return list
    }


    private fun binarySearchIndex(key: ByteBuffer): Long {
        var lo = 0
        var hi = indexList.lastIndex
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val cmp = indexList[mid].first.compareTo(key)
            if (cmp <= 0) lo = mid + 1 else hi = mid - 1
        }
        return if (hi >= 0) indexList[hi].second else -1
    }
}
