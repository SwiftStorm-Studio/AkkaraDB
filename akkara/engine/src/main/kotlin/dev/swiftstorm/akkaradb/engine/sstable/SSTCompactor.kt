/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.format.akk.AkkBlockUnpacker
import dev.swiftstorm.akkaradb.format.api.RecordCursor
import dev.swiftstorm.akkaradb.format.api.RecordView
import java.io.Closeable
import java.lang.Long.compareUnsigned
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.*
import kotlin.io.path.createDirectories
import kotlin.io.path.notExists

class SSTCompactor(
    private val baseDir: Path,
    private val maxPerLevel: Int = 4,
    private val seqClock: ((seq: Long) -> Long?)? = null,
    private val manifest: dev.swiftstorm.akkaradb.engine.manifest.AkkManifest? = null,
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

    private fun listSstFiles(levelDir: Path): List<Path> {
        if (!Files.isDirectory(levelDir)) return emptyList()
        return Files.list(levelDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".sst") }
                .sorted(Comparator.comparing { it.fileName.toString() })
                .toList()
        }
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

        val isBottom = existingLevels().none { it > nextLevel }

        val relInputs = allInputs.map { baseDir.relativize(it).toString() }
        manifest?.compactionStart(level, relInputs)

        val (entries, firstHex, lastHex) = compactInto(allInputs, output, isBottom)

        // Delete inputs after successful write
        (currentLevelFiles + nextLevelFiles).forEach { Files.deleteIfExists(it) }

        val relOut = baseDir.relativize(output).toString()
        manifest?.compactionEnd(nextLevel, relOut, relInputs, entries, firstHex, lastHex)
    }

    private fun newFileName(level: Int): String {
        val ts = System.currentTimeMillis()
        val suffix = UUID.randomUUID().toString().replace("-", "").take(8)
        return "L${level}_${ts}_$suffix.sst"
    }

    private fun compactInto(inputs: List<Path>, output: Path, isBottomLevel: Boolean): Triple<Long, String?, String?> {
        val iterators = inputs.map { SSTIterator(it) }
        try {
            val expected = iterators.sumOf { it.totalEntries }
            FileChannel.open(output, CREATE, TRUNCATE_EXISTING, WRITE, READ).use { ch ->
                SSTableWriter(ch, expected).use { writer ->
                    val merged = merge(
                        iterators = iterators,
                        nowMillis = System.currentTimeMillis(),
                        isBottomLevelCompaction = isBottomLevel,
                        ttlMillis = 24L * 60 * 60 * 1000,
                        seqClock = seqClock
                    )
                    var firstHex: String? = null
                    var lastHex: String? = null
                    val wrapped = sequence {
                        for (r in merged) {
                            if (firstHex == null) firstHex = firstKeyHex(r.key)
                            lastHex = firstKeyHex(r.key)
                            yield(r)
                        }
                    }
                    writer.writeAll(wrapped)
                    val seal = writer.seal()
                    return Triple(seal.entries, firstHex, lastHex)
                }
            }
        } finally {
            iterators.forEach { it.close() }
        }
    }

    private fun merge(iterators: List<SSTIterator>): Sequence<MemRecord> =
        merge(iterators, System.currentTimeMillis(), false, 24L * 60 * 60 * 1000, seqClock)

    private fun firstKeyHex(k: ByteBufferL): String = buildString {
        val d = k.duplicate().position(0)
        var i = 0
        while (d.remaining > 0 && i < 16) {
            append(String.format("%02x", d.i8))
            i++
        }
    }

    private fun merge(
        iterators: List<SSTIterator>,
        nowMillis: Long,
        isBottomLevelCompaction: Boolean,
        ttlMillis: Long = 24L * 60 * 60 * 1000,
        seqClock: ((seq: Long) -> Long?)? = null
    ): Sequence<MemRecord> = sequence {
        if (iterators.isEmpty()) return@sequence

        val comparator = Comparator<HeapEntry> { a, b ->
            val cmp = lexCompare(a.record.key, b.record.key)
            if (cmp != 0) cmp
            else compareUnsigned(b.record.seq, a.record.seq)
        }
        val pq = PriorityQueue(comparator)

        iterators.forEach { it.nextRecord()?.let { r -> pq += HeapEntry(r, it) } }

        while (pq.isNotEmpty()) {
            val first = pq.poll()
            val sameKey = mutableListOf(first)
            while (pq.isNotEmpty()) {
                val peek = pq.peek()
                if (lexCompare(peek.record.key, first.record.key) == 0) {
                    sameKey += pq.poll()
                } else break
            }

            sameKey.forEach { entry ->
                entry.iterator.nextRecord()?.let { next -> pq += HeapEntry(next, entry.iterator) }
            }

            var best: SSTRecord? = null
            for (he in sameKey) {
                val r = he.record
                if (best == null) {
                    best = r
                } else {
                    val sCmp = compareUnsigned(r.seq, best.seq)
                    if (sCmp > 0) {
                        best = r
                    } else if (sCmp == 0) {
                        val rT = r.isTombstone()
                        val bT = best.isTombstone()
                        if (rT && !bT) best = r
                    }
                }
            }
            val winner = best!!

            if (!winner.isTombstone()) {
                yield(winner.toMemRecord())
                continue
            }

            if (!shouldDropTombstone(winner, nowMillis, ttlMillis, isBottomLevelCompaction, seqClock)) {
                yield(winner.toMemRecord())
            }
        }
    }

    private data class HeapEntry(
        val record: SSTRecord,
        val iterator: SSTIterator
    )

    private class SSTIterator(private val path: Path) : Closeable {
        private val channel: FileChannel = FileChannel.open(path, READ)
        private val footer: AKSSFooter.Footer
        private val dataEnd: Long
        private val unpacker = AkkBlockUnpacker()
        private var cursor: RecordCursor? = null
        private var nextBlockOff: Long = 0L
        private val blockBuf: ByteBufferL = ByteBufferL.allocate(BLOCK_SIZE)

        val totalEntries: Long

        init {
            val size = channel.size()
            require(size >= AKSSFooter.SIZE) { "SST too small: $path" }
            val footerBuf = ByteBufferL.allocate(AKSSFooter.SIZE)
            channel.position(size - AKSSFooter.SIZE)
            footerBuf.readFully(channel, AKSSFooter.SIZE)
            footerBuf.position = 0
            footer = AKSSFooter.readFrom(footerBuf)
            dataEnd = footer.indexOff
            totalEntries = footer.entries.toLong()
        }

        fun nextRecord(): SSTRecord? {
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

            val stored = blockBuf.at(BLOCK_SIZE - 4).i32
            val calc = blockBuf.crc32cRange(0, BLOCK_SIZE - 4)
            require(stored == calc) { "CRC32C mismatch at off=$off in $path" }

            blockBuf.position = 0
            blockBuf.limit = BLOCK_SIZE
            cursor = unpacker.cursor(blockBuf.asReadOnlyDuplicate())
        }

        override fun close() {
            channel.close()
        }

        private fun RecordView.toRecord(): SSTRecord {
            val keyCopy = copyBuffer(key)
            val valueCopy = copyBuffer(value)
            return SSTRecord(keyCopy, valueCopy, seq.raw, flags)
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

    private data class SSTRecord(
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

    private fun SSTRecord.deleteAtMillisOrNull(): Long? {
        if (!isTombstone()) return null
        val len = value.remaining
        if (len != 8) return null
        return value.at(0).i64
    }

    private fun shouldDropTombstone(
        tomb: SSTRecord,
        nowMillis: Long,
        ttlMillis: Long,
        isBottomLevelCompaction: Boolean,
        seqClock: ((seq: Long) -> Long?)?
    ): Boolean {
        val stamped = tomb.deleteAtMillisOrNull()
        val approx = if (stamped == null) seqClock?.invoke(tomb.seq) else null
        val delAt = stamped ?: approx

        val expired = (delAt != null) && (nowMillis - delAt >= ttlMillis)
        val canDropHere = isBottomLevelCompaction

        return expired && canDropHere
    }
}
