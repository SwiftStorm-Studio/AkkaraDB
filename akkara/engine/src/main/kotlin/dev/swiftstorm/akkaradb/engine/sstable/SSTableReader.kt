package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.io.Closeable
import java.lang.ref.SoftReference
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import java.util.zip.CRC32C

/**
 * SSTable reader with **fixed-length outer index entries** (ALL Little-Endian).
 *
 * Layout (LE only):
 *  - Footer:  [magic:u32 LE="AKSS"][indexOff:u64 LE][bloomOff:u64 LE]  (20 bytes)
 *  - Index:   repeat { [key:FIXED(32B)][off:i64 LE] }
 *  - Block:   [len:u32 LE][ payload ][crc:u32 LE]
 *  - MiniIdx: [count:u16 LE][offset:u32 LE]×count   // offsets[i] = record-start (payload内先頭からの相対)
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

    companion object {
        private const val FOOTER_SIZE = 20

        // "AKSS" = 0x41 0x4B 0x53 0x53. LE: 0x53534B41
        private const val MAGIC_LE = 0x53534B41
    }

    init {
        require(fileSize >= FOOTER_SIZE) { "file too small: $path size=$fileSize" }

        // ---- footer (ALL LE) ----
        val (idxTmp, bloomTmp) = readFooter(ch)
        indexOff = idxTmp
        bloomOff = bloomTmp

        // ---- basic range checks ----
        val footerPos = fileSize - FOOTER_SIZE
        require(indexOff in 0 until fileSize) { "indexOff out of range: $indexOff / size=$fileSize" }
        require(bloomOff in 0..footerPos) { "bloomOff out of range: $bloomOff / tail=$footerPos" }
        require(indexOff <= bloomOff) { "indexOff($indexOff) > bloomOff($bloomOff)" }

        // ---- bloom: [bits...][hashCount:u32 LE] just before footer ----
        val bloomSize = (footerPos - bloomOff - 4).toInt()
        require(bloomSize >= 0) { "negative bloomSize: $bloomSize (bloomOff=$bloomOff footerPos=$footerPos)" }

        val bloomBuf: MappedByteBuffer = mapRO(ch, bloomOff, bloomSize.toLong()) // bitset本体はエンディアン非依存
        val hcBuf: MappedByteBuffer = mapRO(ch, bloomOff + bloomSize, 4)
        val hashCount = hcBuf.int
        require(hashCount > 0) { "invalid bloom hashCount: $hashCount" }
        bloom = BloomFilter.readFrom(bloomBuf, hashCount)

        // ---- index ----
        val indexSize = (bloomOff - indexOff).toInt()
        require(indexSize >= 0) { "negative indexSize: $indexSize" }
        require(indexSize % OuterIndex.ENTRY_SIZE == 0) { "outer index not aligned: size=$indexSize" }
        val indexBuf = ByteBufferL.wrap(mapRO(ch, indexOff, indexSize.toLong()))
        index = OuterIndex(indexBuf)
    }

    fun mightContain(key: ByteBufferL): Boolean = bloom.mightContain(key)

    fun get(key: ByteBufferL): Record? {
        if (!bloom.mightContain(key)) return null
        val off = index.lookup(key) ?: return null

        // header(len:LE)
        require(off >= 0 && off + 4 <= fileSize) { "block header out of file: off=$off size=$fileSize" }
        val len = readIntLE(ch, off)

        require(len in 0..BLOCK_SIZE) { "bad block len=$len at off=$off" }
        val total = 4 + len + 4
        require(off + total <= fileSize) { "block exceeds file: off=$off total=$total size=$fileSize" }

        val mapped = mapRO(ch, off, total.toLong())

        val payload = ByteBufferL.wrap(mapped.slice().apply { position(4); limit(4 + len) })
        val storedCrc = mapped.getInt(4 + len)
        crc32TL.get().run {
            reset()
            update(payload.asReadOnlyByteBuffer().slice())
            if (value.toInt() != storedCrc) error("CRC mismatch @$off")
        }

        val offsets = miniIndexFor(off, payload)
        val dataStart = payload.position + 2 + offsets.size * 4

        var lo = 0
        var hi = offsets.size - 1
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

    override fun iterator(): Iterator<Record> = object : Iterator<Record> {
        var idxPos = 0
        var recPosInBlock = 0
        var dataStart = 0
        var offsets: IntArray = IntArray(0)
        var payload: ByteBufferL? = null
        var blockOff: Long = -1

        private fun loadNextBlock(): Boolean {
            payload = null
            if (idxPos >= index.countEntries()) return false

            blockOff = index.offsetAt(idxPos)
            idxPos++

            require(blockOff >= 0 && blockOff + 4 <= fileSize) { "block header OOB: off=$blockOff size=$fileSize" }
            val len = readIntLE(ch, blockOff)
            if (len !in 0..BLOCK_SIZE) return false
            val total = 4 + len + 4
            if (blockOff + total > fileSize) return false

            val mapped = mapRO(ch, blockOff, total.toLong())
            val p = ByteBufferL.wrap(mapped.slice().apply { position(4); limit(4 + len) })
            val stored = mapped.getInt(4 + len)
            crc32TL.get().run {
                reset()
                update(p.asReadOnlyByteBuffer().slice())
                if (value.toInt() != stored) error("CRC mismatch @$blockOff")
            }

            val mi = p.duplicate()
            val count = mi.short.toInt() and 0xFFFF
            if (count == 0) {
                payload = null
                return false
            }

            offsets = IntArray(count) { mi.int }
            dataStart = p.position + 2 + 4 * count
            payload = p
            recPosInBlock = 0
            return true
        }

        override fun hasNext(): Boolean {
            if (payload != null && recPosInBlock < offsets.size) return true
            return loadNextBlock()
        }

        override fun next(): Record {
            if (!hasNext()) throw NoSuchElementException()
            val p = payload!!
            val recPos = dataStart + offsets[recPosInBlock++]
            val recBuf = p.duplicate().apply { position(recPos) }
            return AkkRecordReader.read(recBuf)
        }
    }

    fun iterator(
        from: ByteBufferL? = null,
        toExclusive: ByteBufferL? = null
    ): Iterator<Record> = object : Iterator<Record> {
        var idxPos = 0
        var recPosInBlock = 0
        var dataStart = 0
        var offsets: IntArray = IntArray(0)
        var payload: ByteBufferL? = null
        var blockOff: Long = -1
        var finished = false
        val rr = AkkRecordReader

        private fun loadNextBlock(): Boolean {
            payload = null
            while (true) {
                if (idxPos >= index.countEntries()) return false

                blockOff = index.offsetAt(idxPos)
                idxPos++

                if (!(blockOff >= 0 && blockOff + 4 <= fileSize)) continue
                val len = readIntLE(ch, blockOff)
                if (len !in 0..BLOCK_SIZE) continue
                val total = 4 + len + 4
                if (blockOff + total > fileSize) continue

                val mapped = mapRO(ch, blockOff, total.toLong())
                val p = ByteBufferL.wrap(mapped.slice().apply { position(4); limit(4 + len) })
                val stored = mapped.getInt(4 + len)
                crc32TL.get().run {
                    reset()
                    update(p.asReadOnlyByteBuffer().slice())
                    if (value.toInt() != stored) error("CRC mismatch @$blockOff")
                }

                val mi = p.duplicate()
                val count = mi.short.toInt() and 0xFFFF
                if (count == 0) {
                    payload = null
                    continue
                }

                offsets = IntArray(count) { mi.int }
                dataStart = p.position + 2 + 4 * count
                payload = p

                // from下限に対して、このブロック内での lower_bound を求める
                recPosInBlock = if (from == null) 0 else lowerBoundInBlock(from)
                if (recPosInBlock >= offsets.size) {
                    // このブロックは範囲外（すべて smaller）
                    payload = null
                    continue
                }
                return true
            }
        }

        private fun lowerBoundInBlock(from: ByteBufferL): Int {
            val p = payload ?: return 0
            var lo = 0
            var hi = offsets.size
            while (lo < hi) {
                val mid = (lo + hi) ushr 1
                val recPos = dataStart + offsets[mid]
                val recBuf = p.duplicate().apply { position(recPos) }
                val r = rr.read(recBuf)
                val cmp = r.key.compareTo(from)
                if (cmp < 0) lo = mid + 1 else hi = mid
            }
            return lo
        }

        override fun hasNext(): Boolean {
            if (finished) return false
            if (payload != null && recPosInBlock < offsets.size) return true
            return loadNextBlock()
        }

        override fun next(): Record {
            if (!hasNext()) throw NoSuchElementException()
            val p = payload!!
            val recPos = dataStart + offsets[recPosInBlock++]
            val recBuf = p.duplicate().apply { position(recPos) }
            val r = rr.read(recBuf)
            if (toExclusive != null && r.key.compareTo(toExclusive) >= 0) finished = true
            return r
        }
    }

    private fun miniIndexFor(blockOff: Long, payload: ByteBufferL): IntArray {
        synchronized(miniCache) { miniCache[blockOff]?.get()?.let { return it } }
        val mi = payload.duplicate()
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
    private class OuterIndex(private val buf: ByteBufferL) {

        fun lookup(key: ByteBufferL): Long? {
            var lo = 0
            var hi = countEntries() - 1
            while (lo <= hi) {
                val mid = (lo + hi) ushr 1
                val cmp = compareKeyAt(mid, key)
                if (cmp <= 0) lo = mid + 1 else hi = mid - 1
            }
            return if (hi >= 0) offsetAt(hi) else null
        }

        fun countEntries(): Int = buf.limit / ENTRY_SIZE

        private fun compareKeyAt(idx: Int, key: ByteBufferL): Int {
            val pos = idx * ENTRY_SIZE
            val kSlice = buf.duplicate().apply {
                position(pos); limit(pos + FIXED_KEY_SIZE)
            }
            // unsigned lex（提示の compareTo 実装に準拠）
            return kSlice.compareTo(key)
        }

        fun offsetAt(idx: Int): Long {
            val pos = idx * ENTRY_SIZE + FIXED_KEY_SIZE
            val d = buf.duplicate().apply { position(pos) }
            return d.long
        }

        companion object {
            const val FIXED_KEY_SIZE = 32
            const val ENTRY_SIZE = FIXED_KEY_SIZE + 8
        }
    }

    // ---------------- helpers ----------------

    private fun readFooter(ch: FileChannel): Pair<Long, Long> {
        val size = ch.size()
        val f = ByteBufferL.allocate(FOOTER_SIZE)
        ch.read(f.toMutableByteBuffer(), size - FOOTER_SIZE)
        f.flip()

        // magic / offsets (ALL LE)
        val magic = f.int
        require(magic == MAGIC_LE) { "bad footer magic: 0x${magic.toUInt().toString(16)}" }
        val idx = f.long
        val bloom = f.long
        return idx to bloom
    }

    private fun mapRO(
        ch: FileChannel,
        pos: Long,
        len: Long
    ): MappedByteBuffer {
        val size = ch.size()
        require(pos >= 0) { "map pos < 0 : $pos" }
        require(len >= 0) { "map len < 0 : $len" }
        require(pos + len <= size) { "map range overflow: pos=$pos len=$len size=$size" }

        return ch.map(FileChannel.MapMode.READ_ONLY, pos, len).apply {
            order(java.nio.ByteOrder.LITTLE_ENDIAN)
        }
    }

    /** Absolute read of a little-endian 32-bit int at [pos], with read-fully semantics. */
    private fun readIntLE(ch: FileChannel, pos: Long): Int =
        pool.borrow(4) { tmp ->
            tmp.clear()
            val bb = tmp.toMutableByteBuffer()
            var off = 0
            while (off < 4) {
                val n = ch.read(bb, pos + off)
                require(n > 0) { "unexpected EOF while reading int @${pos + off}" }
                off += n
            }
            tmp.flip()
            tmp.int
        }
}
