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
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.zip.CRC32C

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

    /* ───────── in‑memory helpers ───────── */

    private val bloom: BloomFilter
    private val index: IndexBlock
    private val crc32 = CRC32C()

    init {
        // 1) footer
        require(fileSize >= 20) { "file too small: $path" }
        val (idxTmp, bloomTmp) = pool.borrow(20) { f ->
            ch.read(f, fileSize - 20); f.flip()
            require(f.int == 0x414B5353) { "bad footer magic" }
            f.long to f.long
        }
        indexOff = idxTmp; bloomOff = bloomTmp

        // 2) bloom bits (mmap)
        val bloomSize = (fileSize - 20 - bloomOff).toInt()
        bloom = BloomFilter.readFrom(ch.map(FileChannel.MapMode.READ_ONLY, bloomOff, bloomSize.toLong()))

        // 3) index block (mmap)
        val indexSize = (bloomOff - indexOff).toInt()
        index = IndexBlock.readFrom(ch.map(FileChannel.MapMode.READ_ONLY, indexOff, indexSize.toLong()), indexSize)
    }

    /* ───────── public API ───────── */

    fun mightContain(key: ByteBuffer): Boolean = bloom.mightContain(key)

    /** Bloom → outer index → mini‑index binary search → record */
    fun get(key: ByteBuffer): Record? {
        if (!bloom.mightContain(key)) return null
        val off = index.lookup(key)
        if (off < 0) return null

        // 1) read block header (len) – 4B
        val len = pool.borrow(4) { hdr ->
            hdr.clear(); ch.read(hdr, off); hdr.flip(); hdr.int
        }
        require(len in 0..BLOCK_SIZE) { "payload length=$len out of range" }

        val total = 4 + len + 4
        val mapped = ch.map(FileChannel.MapMode.READ_ONLY, off, total.toLong()).order(ByteOrder.BIG_ENDIAN)

        val payload = mapped.slice().apply {
            position(4); limit(4 + len)        // skip len
        }

        val storedCrc = mapped.getInt(4 + len)
        crc32.reset(); crc32.update(payload.duplicate())
        require(storedCrc == crc32.value.toInt()) { "CRC mismatch @$off" }

        // 2) ---- mini‑index ----
        val mini = payload.duplicate()
        val count = VarIntCodec.readInt(mini)
        val offsets = IntArray(count)
        var cumul = 0
        for (i in 0 until count) {
            cumul += VarIntCodec.readInt(mini)       // delta ← stored
            offsets[i] = cumul
        }
        val dataStart = mini.position()              // start of encoded records

        // 3) binary search inside block
        var lo = 0
        var hi = count - 1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val recPos = dataStart + offsets[mid]
            val recBuf = payload.duplicate().apply { position(recPos) }
            val rec = AkkRecordReader.read(recBuf)
            val cmp = rec.key.compareTo(key)
            when {
                cmp == 0 -> return rec
                cmp < 0 -> lo = mid + 1
                else -> hi = mid - 1
            }
        }
        return null
    }

    override fun close() = ch.close()
}