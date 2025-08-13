package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * Levelled compactor (L0 -> L1) using the latest SSTableReader iterator
 * and flags-based tombstone handling.
 */
class Compactor(
    private val levels: MutableList<ConcurrentLinkedDeque<SSTableReader>>, // shared from AkkaraDB
    private val baseDir: Path,
    private val l0Limit: Int = 4,
    private val pool: BufferPool = Pools.io(),
) {
    @Synchronized
    fun maybeCompact() {
        val l0 = levels[0]
        if (l0.size <= l0Limit) return

        // pick oldest (l0Limit+1) files to merge
        val inputs = ArrayList<SSTableReader>(l0Limit + 1)
        repeat(l0Limit + 1) { l0.pollLast()?.let(inputs::add) }
        if (inputs.isEmpty()) return

        // output path & merge
        val outPath = baseDir.resolve("sst_compact_${'$'}{System.nanoTime()}.aksst")
        val merged = mergeSstables(inputs, outPath)

        // L1 deque (create if absent)
        while (levels.size <= 1) levels += ConcurrentLinkedDeque<SSTableReader>()
        levels[1].addFirst(merged)

        // cleanup
        inputs.forEach { sst ->
            sst.close()
            Files.deleteIfExists(sst.path)
        }
    }

    /* ───────── helpers ───────── */

    private fun mergeSstables(src: List<SSTableReader>, out: Path): SSTableReader {
        // min-heap (key asc, seqNo desc)
        val heap = PriorityQueue<Pair<Record, Int>>(
            compareBy<Pair<Record, Int>> { it.first.key }.thenByDescending { it.first.seqNo })

        val iters = src.map { it.iterator() }
        iters.forEachIndexed { idx, it -> if (it.hasNext()) heap += it.next() to idx }

        val latest = LinkedHashMap<Key, Record>()   // Key = content-based wrapper
        while (heap.isNotEmpty()) {
            val (rec, idx) = heap.poll()

            // deep-copy key bytes for map identity
            val keyBytes = ByteArray(rec.key.remaining()).also {
                rec.key.duplicate().get(it)
            }
            val key = Key(keyBytes)

            // keep only the newest SeqNo
            val prev = latest[key]
            if (prev == null || rec.seqNo > prev.seqNo) latest[key] = rec

            if (iters[idx].hasNext()) heap += iters[idx].next() to idx
        }

        val finalRecords = latest.values
            .asSequence()
            .filter { !it.isTombstone }            // flags-based tombstone drop
            .sortedBy { it.key }                   // key asc
            .toList()

        SSTableWriter(out, pool).use { it.write(finalRecords) }
        return SSTableReader(out, pool)
    }
}

/** Byte-array key wrapper for HashMap identity during merge. */
data class Key(private val bytes: ByteArray) {
    override fun equals(other: Any?): Boolean = other is Key && bytes.contentEquals(other.bytes)
    override fun hashCode(): Int = bytes.contentHashCode()
}
