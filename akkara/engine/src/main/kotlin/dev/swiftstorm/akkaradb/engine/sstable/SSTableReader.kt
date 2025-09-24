@file:Suppress("DuplicatedCode", "ReplaceCallWithBinaryOperator")

package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.engine.util.BloomFilter
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.io.Closeable
import java.io.EOFException
import java.lang.ref.SoftReference
import java.nio.ByteBuffer
import java.nio.ByteOrder
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
 *  - Block:   [len:u32 LE=BLOCK_SIZE][payload=BLOCK_SIZE padded][crc:u32 LE]
 *  - MiniIdx: [count:u16 LE][offset:u32 LE]×count
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
        private const val MAGIC_LE = 0x53534B41 // "AKSS" little-endian
    }

    init {
        require(fileSize >= FOOTER_SIZE) { "file too small: $path size=$fileSize" }

        val (idxTmp, bloomTmp) = readFooter(ch)
        indexOff = idxTmp
        bloomOff = bloomTmp

        val footerPos = fileSize - FOOTER_SIZE
        require(indexOff in 0 until fileSize) { "indexOff out of range: $indexOff / size=$fileSize" }
        require(bloomOff in 0..footerPos) { "bloomOff out of range: $bloomOff / tail=$footerPos" }
        require(indexOff <= bloomOff) { "indexOff($indexOff) > bloomOff($bloomOff)" }

        // Bloom
        val bloomSize = (footerPos - bloomOff - 4).toInt()
        require(bloomSize >= 0) { "negative bloomSize: $bloomSize (bloomOff=$bloomOff footerPos=$footerPos)" }
        val bloomBuf: MappedByteBuffer = mapRO(ch, bloomOff, bloomSize.toLong())
        val hcBuf: MappedByteBuffer = mapRO(ch, bloomOff + bloomSize, 4)
        val hashCount = hcBuf.int
        require(hashCount > 0) { "invalid bloom hashCount: $hashCount" }
        bloom = BloomFilter.readFrom(ByteBufferL.wrap(bloomBuf), hashCount)

        // Index
        val indexSize = (bloomOff - indexOff).toInt()
        require(indexSize % OuterIndex.ENTRY_SIZE == 0) { "outer index not aligned: size=$indexSize" }
        val indexBuf = ByteBufferL.wrap(mapRO(ch, indexOff, indexSize.toLong()))
        index = OuterIndex(indexBuf)
    }

    fun get(key: ByteBufferL): Record? {
        if (!bloom.mightContain(key)) return null
        val off = index.lookup(key) ?: return null

        // Always BLOCK_SIZE padded block
        require(off >= 0 && off + 4 <= fileSize) { "block header out of file: off=$off size=$fileSize" }
        val len = readIntLE(ch, off)
        require(len == BLOCK_SIZE) { "unexpected block len=$len (expected=$BLOCK_SIZE)" }

        val total = 4 + BLOCK_SIZE + 4
        require(off + total <= fileSize) { "block exceeds file: off=$off total=$total size=$fileSize" }

        val mapped = mapRO(ch, off, total.toLong())

        val payload = ByteBufferL.wrap(mapped.slice().apply { position(4); limit(4 + BLOCK_SIZE) })
        val storedCrc = mapped.getInt(4 + BLOCK_SIZE)
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

    fun scanRange(
        startInclusive: ByteBufferL,
        endExclusive: ByteBufferL? = null
    ): Iterator<Record> = object : Iterator<Record> {
        var idxPos: Int
        var recPosInBlock = 0
        var dataStart = 0
        var offsets: IntArray = IntArray(0)
        var payload: ByteBufferL? = null
        var blockOff: Long = -1
        var initialized = false

        init {
            val lb = index.lowerBound(startInclusive)
            idxPos = (lb - 1).coerceAtLeast(0)
        }

        private fun loadBlockAt(off: Long): Boolean {
            val len = readIntLE(ch, off)
            if (len != BLOCK_SIZE) return false
            val total = 4 + BLOCK_SIZE + 4
            if (off + total > fileSize) return false

            val mapped = mapRO(ch, off, total.toLong())
            val p = ByteBufferL.wrap(mapped.slice().apply { position(4); limit(4 + BLOCK_SIZE) })
            val stored = mapped.getInt(4 + BLOCK_SIZE)
            crc32TL.get().run {
                reset()
                update(p.asReadOnlyByteBuffer().slice())
                if (value.toInt() != stored) error("CRC mismatch @$off")
            }
            payload = p
            offsets = miniIndexFor(off, p)
            dataStart = p.position + 2 + 4 * offsets.size
            recPosInBlock = 0
            return true
        }

        private fun loadNextBlock(): Boolean {
            payload = null
            if (idxPos >= index.countEntries()) return false
            blockOff = index.offsetAt(idxPos++)
            return loadBlockAt(blockOff)
        }

        private fun seekWithinBlockToStart(): Boolean {
            val p = payload ?: return false
            var lo = 0
            var hi = offsets.size - 1
            var ans = offsets.size
            while (lo <= hi) {
                val mid = (lo + hi) ushr 1
                val recPos = dataStart + offsets[mid]
                val recBuf = p.duplicate().apply { position(recPos) }
                val rec = AkkRecordReader.read(recBuf)
                val cmp = rec.key.compareTo(startInclusive)
                if (cmp < 0) lo = mid + 1 else {
                    ans = mid; hi = mid - 1
                }
            }
            recPosInBlock = ans
            return recPosInBlock < offsets.size
        }

        private fun ensureInitialized(): Boolean {
            if (initialized) return true
            initialized = true

            if (!loadNextBlock()) return false

            if (!seekWithinBlockToStart()) {
                while (loadNextBlock()) {
                    if (seekWithinBlockToStart()) break
                }
            }
            return payload != null
        }

        override fun hasNext(): Boolean {
            if (!ensureInitialized()) return false

            if (endExclusive != null && payload != null && recPosInBlock < offsets.size) {
                val p = payload!!
                val recPos = dataStart + offsets[recPosInBlock]
                val recBuf = p.duplicate().apply { position(recPos) }
                val next = AkkRecordReader.read(recBuf)
                if (next.key >= endExclusive) return false
            }

            if (payload != null && recPosInBlock < offsets.size) return true

            while (loadNextBlock()) {
                recPosInBlock = 0
                if (offsets.isNotEmpty()) return true
            }
            return false
        }

        override fun next(): Record {
            if (!hasNext()) throw NoSuchElementException()
            val p = payload!!
            val recPos = dataStart + offsets[recPosInBlock++]
            val recBuf = p.duplicate().apply { position(recPos) }
            val r = AkkRecordReader.read(recBuf)
            if (endExclusive != null && r.key.compareTo(endExclusive) >= 0) {
                throw NoSuchElementException()
            }
            return r
        }
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

            val len = readIntLE(ch, blockOff)
            if (len != BLOCK_SIZE) return false

            val total = 4 + BLOCK_SIZE + 4
            if (blockOff + total > fileSize) return false

            val mapped = mapRO(ch, blockOff, total.toLong())
            val p = ByteBufferL.wrap(mapped.slice().apply { position(4); limit(4 + BLOCK_SIZE) })
            val stored = mapped.getInt(4 + BLOCK_SIZE)
            crc32TL.get().run {
                reset()
                update(p.asReadOnlyByteBuffer().slice())
                if (value.toInt() != stored) error("CRC mismatch @$blockOff")
            }

            val mi = p.duplicate()
            val count = mi.short.toInt() and 0xFFFF
            if (count == 0) return false

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

    private fun miniIndexFor(blockOff: Long, payload: ByteBufferL): IntArray {
        synchronized(miniCache) { miniCache[blockOff]?.get()?.let { return it } }
        val mi = payload.duplicate()
        val count = mi.short.toInt() and 0xFFFF
        val arr = IntArray(count) { mi.int }
        synchronized(miniCache) { miniCache[blockOff] = SoftReference(arr) }
        return arr
    }

    override fun close() = ch.close()

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
            return kSlice.compareTo(key)
        }

        fun offsetAt(idx: Int): Long {
            val pos = idx * ENTRY_SIZE + FIXED_KEY_SIZE
            val d = buf.duplicate().apply { position(pos) }
            return d.long
        }

        fun lowerBound(target: ByteBufferL): Int {
            var lo = 0
            var hi = countEntries() - 1
            var ans = countEntries()
            while (lo <= hi) {
                val mid = (lo + hi) ushr 1
                val cmp = compareKeyAt(mid, target)
                if (cmp < 0) lo = mid + 1 else {
                    ans = mid; hi = mid - 1
                }
            }
            return ans
        }

        companion object {
            const val FIXED_KEY_SIZE = 32
            const val ENTRY_SIZE = FIXED_KEY_SIZE + 8
        }
    }

    // ---------------- helpers ----------------

    private fun readFooter(ch: FileChannel): Pair<Long, Long> {
        val size = ch.size()
        require(size >= FOOTER_SIZE) { "file too small: $size" }
        val f = ByteBufferL.allocate(FOOTER_SIZE)
        val bb = f.toMutableByteBuffer()
        ch.readFully(bb, size - FOOTER_SIZE)
        bb.flip()
        val magic = bb.int
        require(magic == MAGIC_LE) { "bad footer magic: 0x${magic.toUInt().toString(16)}" }
        return bb.long to bb.long
    }

    private fun mapRO(ch: FileChannel, pos: Long, len: Long): MappedByteBuffer {
        return ch.map(FileChannel.MapMode.READ_ONLY, pos, len).apply { order(ByteOrder.LITTLE_ENDIAN) }
    }

    private fun readIntLE(ch: FileChannel, pos: Long): Int =
        pool.borrow(4) { tmp ->
            tmp.clear()
            val bb = tmp.getByteBuffer()
            var off = 0
            while (off < 4) {
                val n = ch.read(bb, pos + off)
                require(n > 0) { "unexpected EOF while reading int @${pos + off}" }
                off += n
            }
            bb.flip()
            bb.int
        }

    private fun FileChannel.readFully(dst: ByteBuffer, position: Long) {
        var pos = position
        while (dst.hasRemaining()) {
            val n = this.read(dst, pos)
            if (n < 0) throw EOFException("Unexpected EOF while reading at pos=$pos")
            pos += n
        }
    }
}
