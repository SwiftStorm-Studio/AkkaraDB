package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.io.Closeable
import java.lang.ref.SoftReference
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.zip.CRC32C

/**
 * SSTable reader with **fixed-length outer index entries**:
 *  - Footer:  [magic:u32][indexOff:u64][bloomOff:u64] (20 bytes, magic="AKSS")
 *  - Index:   repeat { [key:FIXED(32B)][off:i64] } (little-endian)
 *  - Block:   [len:u32 BE][ payload ][crc:u32 BE]
 *  - MiniIdx: [count:u16][offset:u32]×count (little-endian)
 */
class SSTableReader(
    val path: Path,
    private val pool: BufferPool = Pools.io(),
) : Closeable, Iterable<Record> {

    internal val ch: FileChannel = FileChannel.open(path, READ)
    private val fileSize: Long = ch.size()

    private val indexOff: Long
    private val bloomOff: Long

    private val bloom: BloomFilter
    private val index: OuterIndex

    private val crc32TL: ThreadLocal<CRC32C> = ThreadLocal.withInitial { CRC32C() }

    private val miniCache = object : LinkedHashMap<Long, SoftReference<IntArray>>(512, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long, SoftReference<IntArray>>): Boolean = size > 512
    }

    init {
        require(fileSize >= 20) { "file too small: $path" }
        val (idxTmp, bloomTmp) = pool.borrow(20) { f ->
            ch.read(f, fileSize - 20); f.flip()
            require(f.int == 0x414B5353) { "bad footer magic" }
            f.long to f.long
        }
        indexOff = idxTmp; bloomOff = bloomTmp

        val bloomSize = (fileSize - 20 - bloomOff - 4).toInt()
        val bloomBuf = ch.map(FileChannel.MapMode.READ_ONLY, bloomOff, bloomSize.toLong())
        val hashCountBuf = ch.map(FileChannel.MapMode.READ_ONLY, bloomOff + bloomSize, 4)
        hashCountBuf.order(ByteOrder.BIG_ENDIAN)
        val hashCount = hashCountBuf.int
        bloom = BloomFilter.readFrom(bloomBuf, hashCount)

        val indexSize = (bloomOff - indexOff).toInt()
        val indexBuf = ch.map(FileChannel.MapMode.READ_ONLY, indexOff, indexSize.toLong()).order(ByteOrder.LITTLE_ENDIAN)
        index = OuterIndex(indexBuf)
    }

    fun mightContain(key: ByteBuffer): Boolean = bloom.mightContain(key)

    fun get(key: ByteBuffer): Record? {
        if (!bloom.mightContain(key)) return null
        val off = index.lookup(key) ?: return null

        val len = pool.borrow(4) { hdr ->
            hdr.clear(); ch.read(hdr, off); hdr.flip(); hdr.int
        }
        require(len in 0..BLOCK_SIZE)
        val total = 4 + len + 4
        val mapped = ch.map(FileChannel.MapMode.READ_ONLY, off, total.toLong()).order(ByteOrder.BIG_ENDIAN)

        val payload = mapped.slice().apply { position(4); limit(4 + len) }
        val storedCrc = mapped.getInt(4 + len)
        crc32TL.get().run { reset(); update(payload.duplicate()); if (value.toInt() != storedCrc) error("CRC mismatch @$off") }

        val offsets = miniIndexFor(off, payload)
        val dataStart = payload.position() + 2 + offsets.size * 4

        var lo = 0
        var hi = offsets.size - 1
        while (lo <= hi) {
            val mid = (lo + hi) ushr 1
            val recPos = dataStart + offsets[mid]
            val recBuf = payload.duplicate().order(ByteOrder.LITTLE_ENDIAN).apply { position(recPos) }
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

    override fun iterator(): Iterator<Record> = object : Iterator<Record> {
        var idxPos = 0                     // entry index in outer index
        var recPosInBlock = 0              // record index inside current block
        var dataStart = 0
        var offsets: IntArray = IntArray(0)
        var payload: ByteBuffer? = null
        var blockOff: Long = -1

        private fun loadNextBlock(): Boolean {
            // release reference to previous payload; underlying mmap stays alive
            payload = null
            if (idxPos >= index.countEntries()) return false

            blockOff = index.offsetAt(idxPos)
            idxPos++

            // read block header and payload, verify CRC
            val len = pool.borrow(4) { hdr ->
                hdr.clear(); ch.read(hdr, blockOff); hdr.flip(); hdr.int
            }
            if (len !in 0..BLOCK_SIZE) return false
            val total = 4 + len + 4
            val mapped = ch.map(FileChannel.MapMode.READ_ONLY, blockOff, total.toLong()).order(ByteOrder.BIG_ENDIAN)
            val p = mapped.slice().apply { position(4); limit(4 + len) }
            val stored = mapped.getInt(4 + len)
            crc32TL.get().run { reset(); update(p.duplicate()); if (value.toInt() != stored) error("CRC mismatch @$blockOff") }

            // mini-index
            val mi = p.duplicate().order(ByteOrder.LITTLE_ENDIAN)
            val count = mi.short.toInt() and 0xFFFF
            offsets = IntArray(count) { mi.int }
            dataStart = p.position() + 2 + 4 * count

            recPosInBlock = 0
            payload = p
            return count > 0
        }

        override fun hasNext(): Boolean {
            if (payload == null || recPosInBlock >= offsets.size) {
                while (payload == null || recPosInBlock >= offsets.size) {
                    if (!loadNextBlock()) return false
                }
            }
            return true
        }

        override fun next(): Record {
            if (!hasNext()) throw NoSuchElementException()
            val p = payload!!
            val recPos = dataStart + offsets[recPosInBlock++]
            val recBuf = p.duplicate().order(ByteOrder.LITTLE_ENDIAN).apply { position(recPos) }
            return AkkRecordReader.read(recBuf)
        }
    }

    fun iterator(
        from: ByteBuffer? = null,
        toExclusive: ByteBuffer? = null
    ): Iterator<Record> = object : Iterator<Record> {
        var idxPos = 0                   // outer index entry
        var recPosInBlock = 0            // record index inside current block
        var dataStart = 0
        var offsets: IntArray = IntArray(0)
        var payload: ByteBuffer? = null
        var blockOff: Long = -1
        var finished = false
        val rr = AkkRecordReader

        private fun cmp(a: ByteBuffer, b: ByteBuffer): Int {
            val aa = a.duplicate().apply { rewind() }
            val bb = b.duplicate().apply { rewind() }
            while (aa.hasRemaining() && bb.hasRemaining()) {
                val x = aa.get().toInt() and 0xFF
                val y = bb.get().toInt() and 0xFF
                if (x != y) return x - y
            }
            return aa.remaining() - bb.remaining()
        }

        private fun loadNextBlock(): Boolean {
            payload = null
            while (true) {
                if (idxPos >= index.countEntries()) return false

                blockOff = index.offsetAt(idxPos)
                idxPos++

                // ---- read block header + payload, verify CRC ----
                val len = pool.borrow(4) { hdr ->
                    hdr.clear(); ch.read(hdr, blockOff); hdr.flip(); hdr.int
                }
                if (len !in 0..BLOCK_SIZE) continue
                val total = 4 + len + 4
                val mapped = ch.map(FileChannel.MapMode.READ_ONLY, blockOff, total.toLong())
                    .order(ByteOrder.BIG_ENDIAN)
                val p = mapped.slice().apply { position(4); limit(4 + len) }
                val stored = mapped.getInt(4 + len)
                crc32TL.get().run { reset(); update(p.duplicate()); if (value.toInt() != stored) error("CRC mismatch @$blockOff") }

                val mi = p.duplicate().order(ByteOrder.LITTLE_ENDIAN)
                val count = mi.short.toInt() and 0xFFFF
                if (count == 0) {
                    payload = null; continue
                }

                offsets = IntArray(count) { mi.int }
                dataStart = p.position() + 2 + 4 * count
                payload = p

                recPosInBlock = 0
                if (from != null) {
                    recPosInBlock = lowerBoundInBlock(from)
                    if (recPosInBlock >= offsets.size) {
                        payload = null
                        continue
                    }
                }
                return true
            }
        }

        private fun lowerBoundInBlock(from: ByteBuffer): Int {
            val p = payload ?: return 0
            var lo = 0
            var hi = offsets.size
            while (lo < hi) {
                val mid = (lo + hi) ushr 1
                val recPos = dataStart + offsets[mid]
                val recBuf = p.duplicate().order(ByteOrder.LITTLE_ENDIAN).apply { position(recPos) }
                val r = rr.read(recBuf)
                val c = cmp(r.key, from)
                if (c < 0) lo = mid + 1 else hi = mid
            }
            return lo
        }

        override fun hasNext(): Boolean {
            if (finished) return false

            while (payload == null || recPosInBlock >= offsets.size) {
                if (!loadNextBlock()) {
                    finished = true; return false
                }
            }

            if (toExclusive != null) {
                val p = payload!!
                val recPos = dataStart + offsets[recPosInBlock]
                val recBuf = p.duplicate().order(ByteOrder.LITTLE_ENDIAN).apply { position(recPos) }
                val r = rr.read(recBuf)
                if (cmp(r.key, toExclusive) >= 0) {
                    finished = true
                    return false
                }
            }
            return true
        }

        override fun next(): Record {
            if (!hasNext()) throw NoSuchElementException()
            val p = payload!!
            val recPos = dataStart + offsets[recPosInBlock++]
            val recBuf = p.duplicate().order(ByteOrder.LITTLE_ENDIAN).apply { position(recPos) }
            val r = rr.read(recBuf)

            if (toExclusive != null && cmp(r.key, toExclusive) >= 0) finished = true
            return r
        }
    }

    private fun miniIndexFor(blockOff: Long, payload: ByteBuffer): IntArray {
        synchronized(miniCache) { miniCache[blockOff]?.get()?.let { return it } }
        val mi = payload.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        val count = mi.short.toInt() and 0xFFFF
        val arr = IntArray(count)
        for (i in 0 until count) arr[i] = mi.int
        synchronized(miniCache) { miniCache[blockOff] = SoftReference(arr) }
        return arr
    }

    override fun close() = ch.close()

    /**
     * Outer index of fixed-size entries, each consisting of a 32-byte key prefix
     * followed by an i64 block offset (little-endian).
     */
    private class OuterIndex(private val buf: ByteBuffer) {
        init {
            require(buf.order() == ByteOrder.LITTLE_ENDIAN)
        }

        fun lookup(key: ByteBuffer): Long? {
            var lo = 0
            var hi = countEntries() - 1
            while (lo <= hi) {
                val mid = (lo + hi) ushr 1
                val cmp = compareKeyAt(mid, key)
                if (cmp <= 0) lo = mid + 1 else hi = mid - 1
            }
            return if (hi >= 0) offsetAt(hi) else null
        }

        fun countEntries(): Int = buf.limit() / ENTRY_SIZE

        private fun compareKeyAt(idx: Int, key: ByteBuffer): Int {
            val pos = idx * ENTRY_SIZE
            val kSlice = buf.duplicate().apply { position(pos); limit(pos + FIXED_KEY_SIZE) }
            return kSlice.compareTo(key)
        }

        fun offsetAt(idx: Int): Long {
            val offPos = idx * ENTRY_SIZE + FIXED_KEY_SIZE
            return buf.getLong(offPos)
        }

        companion object {
            const val FIXED_KEY_SIZE = 32
            const val ENTRY_SIZE = FIXED_KEY_SIZE + 8
        }
    }
}
