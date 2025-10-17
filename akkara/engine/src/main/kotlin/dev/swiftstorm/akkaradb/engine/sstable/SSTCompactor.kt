package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.MemRecord
import dev.swiftstorm.akkaradb.common.RecordFlags
import dev.swiftstorm.akkaradb.common.lexCompare
import dev.swiftstorm.akkaradb.common.memRecordOf
import dev.swiftstorm.akkaradb.engine.sstable.FOOTER_SIZE
import dev.swiftstorm.akkaradb.engine.sstable.SSTableReader.Footer
import dev.swiftstorm.akkaradb.format.akk.AkkBlockUnpacker
import dev.swiftstorm.akkaradb.format.api.RecordCursor
import dev.swiftstorm.akkaradb.format.api.RecordView
import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import java.util.Comparator
import java.util.PriorityQueue
import java.util.UUID
import kotlin.io.path.createDirectories
import kotlin.io.path.notExists

class SSTCompactor(
    private val baseDir: Path,
    private val maxPerLevel: Int = 4
) {

    init {
        require(maxPerLevel > 0) { "maxPerLevel must be > 0" }
    }

    fun compact() {
        if (baseDir.notExists()) {
            Files.createDirectories(baseDir)
        }
        while (true) {
            val levels = existingLevels()
            if (levels.isEmpty()) break
            val levelToCompact = levels.firstOrNull { level ->
                listSstFiles(levelPath(level)).size > maxPerLevel
            } ?: break
            compactLevel(levelToCompact)
        }
    }

    private fun existingLevels(): List<Int> {
        val levels = mutableSetOf<Int>()
        levels += 0
        if (Files.exists(baseDir)) {
            Files.list(baseDir).use { stream ->
                stream.filter { Files.isDirectory(it) && it.fileName.toString().startsWith("L") }
                    .forEach { dir ->
                        val name = dir.fileName.toString()
                        val lvl = name.substring(1).toIntOrNull()
                        if (lvl != null) levels += lvl
                    }
            }
        }
        return levels.sorted()
    }

    private fun levelPath(level: Int): Path = baseDir.resolve("L$level")

    private fun listSstFiles(levelDir: Path): MutableList<Path> {
        if (!Files.isDirectory(levelDir)) return mutableListOf()
        val files = mutableListOf<Path>()
        Files.list(levelDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".sst") }
                .forEach { files += it }
        }
        files.sortBy { it.fileName.toString() }
        return files
    }

    private fun compactLevel(level: Int) {
        val currentLevelFiles = listSstFiles(levelPath(level))
        if (currentLevelFiles.size <= maxPerLevel) return

        val nextLevel = level + 1
        val nextLevelPath = levelPath(nextLevel)
        nextLevelPath.createDirectories()

        val nextLevelFiles = listSstFiles(nextLevelPath)
        val allInputs = (currentLevelFiles + nextLevelFiles)
        if (allInputs.isEmpty()) return

        val output = nextLevelPath.resolve(newFileName(nextLevel))
        compactInto(allInputs, output)

        (currentLevelFiles + nextLevelFiles).forEach { Files.deleteIfExists(it) }
    }

    private fun newFileName(level: Int): String {
        val ts = System.currentTimeMillis()
        val suffix = UUID.randomUUID().toString().replace("-", "").take(8)
        return "L${level}_$ts_$suffix.sst"
    }

    private fun compactInto(inputs: List<Path>, output: Path) {
        val iterators = inputs.map { SstIterator(it) }
        try {
            val expected = iterators.sumOf { it.totalEntries }
            FileChannel.open(output, CREATE, TRUNCATE_EXISTING, WRITE).use { ch ->
                SSTableWriter(ch, expected).use { writer ->
                    val merged = merge(iterators)
                    writer.writeAll(merged)
                    writer.seal()
                }
            }
        } finally {
            iterators.forEach { it.close() }
        }
    }

    private fun merge(iterators: List<SstIterator>): Sequence<MemRecord> = sequence {
        if (iterators.isEmpty()) return@sequence
        val comparator = Comparator<HeapEntry> { a, b ->
            val cmp = lexCompare(a.record.key, b.record.key)
            if (cmp != 0) cmp else java.lang.Long.compareUnsigned(b.record.seq, a.record.seq)
        }
        val pq = PriorityQueue(comparator)

        iterators.forEach { iterator ->
            iterator.nextRecord()?.let { record ->
                pq += HeapEntry(record, iterator)
            }
        }

        while (pq.isNotEmpty()) {
            val first = pq.poll()
            val sameKey = mutableListOf(first)
            while (pq.isNotEmpty()) {
                val peek = pq.peek()
                if (lexCompare(peek.record.key, first.record.key) == 0) {
                    sameKey += pq.poll()
                } else {
                    break
                }
            }

            sameKey.forEach { entry ->
                val next = entry.iterator.nextRecord()
                if (next != null) {
                    pq += HeapEntry(next, entry.iterator)
                }
            }

            val winner = sameKey.first()
            if (!winner.record.isTombstone()) {
                yield(winner.record.toMemRecord())
            }
        }
    }

    private data class HeapEntry(
        val record: SstRecord,
        val iterator: SstIterator
    )

    private class SstIterator(private val path: Path) : Closeable {
        private val channel: FileChannel = FileChannel.open(path, READ)
        private val footer: Footer
        private val dataEnd: Long
        private val unpacker = AkkBlockUnpacker()
        private var cursor: RecordCursor? = null
        private var nextBlockOff: Long = 0L
        private val blockBuf: ByteBufferL = ByteBufferL.allocate(BLOCK_SIZE)

        val totalEntries: Long

        init {
            val size = channel.size()
            require(size >= FOOTER_SIZE) { "SST too small: $path" }
            val footerBuf = ByteBufferL.allocate(FOOTER_SIZE)
            channel.position(size - FOOTER_SIZE)
            footerBuf.readFully(channel, FOOTER_SIZE)
            footerBuf.position = 0
            footer = Footer.readFrom(footerBuf)
            dataEnd = footer.indexOff
            totalEntries = footer.entries.toLong()
        }

        fun nextRecord(): SstRecord? {
            while (true) {
                val cur = cursor
                if (cur != null) {
                    val view = cur.tryNext()
                    if (view != null) return view.toRecord()
                    cursor = null
                }
                if (nextBlockOff >= dataEnd) return null
                readBlock(nextBlockOff)
                nextBlockOff += BLOCK_SIZE
            }
        }

        private fun readBlock(off: Long) {
            blockBuf.clear()
            blockBuf.limit = BLOCK_SIZE
            channel.position(off)
            blockBuf.readFully(channel, BLOCK_SIZE)
            blockBuf.position = 0
            blockBuf.limit = BLOCK_SIZE
            cursor = unpacker.cursor(blockBuf.asReadOnlyDuplicate())
        }

        override fun close() {
            channel.close()
        }

        private fun RecordView.toRecord(): SstRecord {
            val keyCopy = copyBuffer(key)
            val valueCopy = copyBuffer(value)
            return SstRecord(keyCopy, valueCopy, seq.raw, flags)
        }

        private fun copyBuffer(src: ByteBufferL): ByteBufferL {
            val dup = src.duplicate()
            dup.position = 0
            val len = dup.remaining
            val dst = ByteBufferL.allocate(len, direct = false)
            if (len > 0) {
                dst.put(dup, len)
            }
            dst.position = 0
            dst.limit = len
            return dst
        }
    }

    private data class SstRecord(
        val key: ByteBufferL,
        val value: ByteBufferL,
        val seq: Long,
        val flags: Int
    ) {
        fun isTombstone(): Boolean = (flags and RecordFlags.TOMBSTONE.toInt()) != 0

        fun toMemRecord(): MemRecord {
            val keyDup = key.duplicate().also { it.position = 0 }
            val valueDup = value.duplicate().also { it.position = 0 }
            return memRecordOf(keyDup, valueDup, seq, flags.toByte())
        }
    }

}
