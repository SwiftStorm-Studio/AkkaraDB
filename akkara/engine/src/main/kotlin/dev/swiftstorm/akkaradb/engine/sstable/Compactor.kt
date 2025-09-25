package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.math.max
import kotlin.math.min

class Compactor(
    private val levels: MutableList<ConcurrentLinkedDeque<SSTableReader>>, // shared from AkkaraDB
    private val baseDir: Path,
    private val manifest: AkkManifest,
    private val l0Limit: Int = 4,
    private val fanIn: Int = 4,
    private val levelLimits: IntArray = intArrayOf( /* L0= */ l0Limit, /* L1+= */ 12),
    private val pool: BufferPool = Pools.io(),
) {
    @Synchronized
    fun maybeCompact() {
        ensureLevel(0)

        var progressed = false

        val l0 = levels[0]
        if (l0.size > l0Limit) {
            val count = min(l0.size, l0Limit + 1)
            val inputs = takeOldest(l0, count)
            compactInto(level = 1, inputs = inputs)
            progressed = true
        }

        var level = 1
        while (true) {
            ensureLevel(level)
            val li = levels[level]
            val limit = limitFor(level)
            if (li.size > limit) {
                val count = min(li.size, max(2, fanIn))
                val inputs = takeOldest(li, count)
                compactInto(level = level + 1, inputs = inputs)
                progressed = true
                level = max(1, level)
                continue
            }
            level += 1
            if (level >= levels.size) break
        }
        if (progressed) {
            manifest.checkpoint("compact")
        }
    }

    /* ───────── helpers ───────── */
    private fun ensureLevel(level: Int) {
        while (levels.size <= level) {
            levels += ConcurrentLinkedDeque<SSTableReader>()
        }
    }

    private fun limitFor(level: Int): Int =
        if (level < levelLimits.size) levelLimits[level] else levelLimits.last()

    private fun takeOldest(deque: ConcurrentLinkedDeque<SSTableReader>, n: Int): List<SSTableReader> {
        val out = ArrayList<SSTableReader>(n)
        repeat(n) {
            val s = deque.pollLast() ?: return@repeat
            out += s
        }
        return out
    }

    private fun compactInto(level: Int, inputs: List<SSTableReader>) {
        if (inputs.isEmpty()) return

        val outPath = baseDir.resolve("sst_compact_L${level}_${System.nanoTime()}.aksst")
        val (merged, outEntries) = mergeSstables(inputs, outPath)

        ensureLevel(level)
        levels[level].addFirst(merged)

        manifest.sstSeal(
            level = level,
            file = outPath.fileName.toString(),
            entries = outEntries.toLong(),
            firstKeyHex = null,
            lastKeyHex = null
        )

        inputs.forEach { sst ->
            try {
                sst.close()
            } catch (_: Throwable) {
            }
            try {
                Files.deleteIfExists(sst.path)
            } catch (_: Throwable) {
            }
        }
    }

    private data class Entry(val rec: Record, val src: Int)

    private fun mergeSstables(src: List<SSTableReader>, out: Path): Pair<SSTableReader, Int> {
        val heap = PriorityQueue<Entry> { a, b ->
            val kc = a.rec.key.compareTo(b.rec.key)
            if (kc != 0) kc else b.rec.seqNo.compareTo(a.rec.seqNo)
        }
        val iters = src.map { it.iterator() }
        for (i in iters.indices) if (iters[i].hasNext()) heap += Entry(iters[i].next(), i)

        val writer = SSTableWriter(out, pool)
        val batch = ArrayList<Record>(8192)
        var outCount = 0
        var lastKey: ByteBufferL? = null

        while (heap.isNotEmpty()) {
            val first = heap.poll()
            val curKey = first.rec.key
            var best = first.rec
            if (iters[first.src].hasNext()) heap += Entry(iters[first.src].next(), first.src)

            while (heap.isNotEmpty() && heap.peek().rec.key.compareTo(curKey) == 0) {
                val e = heap.poll()
                if (e.rec.seqNo > best.seqNo) best = e.rec
                if (iters[e.src].hasNext()) heap += Entry(iters[e.src].next(), e.src)
            }

            if (!best.isTombstone && (lastKey == null || lastKey.compareTo(best.key) != 0)) {
                batch += best
                outCount++
                lastKey = best.key
                if (batch.size >= 8192) {
                    writer.write(batch)
                    batch.clear()
                }
            }
        }
        if (batch.isNotEmpty()) writer.write(batch)
        writer.close()

        val reader = SSTableReader(out, pool)
        return reader to outCount
    }
}
