package dev.swiftstorm.akkaradb.engine.sstable

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * Levelled compactor (L0 -> L1).
 *
 * ポイント:
 *  - k-way マージを「同一キーをその場で束ねる」方式で実装（配列コピー/HashMap不要）
 *  - 最新 seq のみ採用、tombstone は drop
 *  - 出力はキー昇順でそのまま SST へ書き出し（後ソート不要）
 *  - Compact 完了時に manifest へ SSTSeal(level=1) と checkpoint("compact") を記録
 */
class Compactor(
    private val levels: MutableList<ConcurrentLinkedDeque<SSTableReader>>, // shared from AkkaraDB
    private val baseDir: Path,
    private val manifest: AkkManifest,
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
        val outPath = baseDir.resolve("sst_compact_${System.nanoTime()}.aksst")
        val (merged, outEntries) = mergeSstables(inputs, outPath)

        // L1 deque (create if absent)
        while (levels.size <= 1) levels += ConcurrentLinkedDeque<SSTableReader>()
        levels[1].addFirst(merged)

        // Manifest: L1 SST 封印 + checkpoint
        manifest.sstSeal(
            level = 1,
            file = outPath.fileName.toString(),
            entries = outEntries.toLong(),
            firstKeyHex = null,   // 爆速優先: 変換省略（必要なら後で付ける）
            lastKeyHex = null
        )
        manifest.checkpoint("compact")

        // cleanup sources
        inputs.forEach { sst ->
            sst.close()
            Files.deleteIfExists(sst.path)
        }
    }

    /* ───────── helpers ───────── */

    private data class Entry(val rec: Record, val src: Int)

    /**
     * 複数 SST をキー昇順でストリーミング結合し、同一キー群から最大 seq の1件のみ採用。
     * tombstone は drop。出力は自然にキー昇順になる。
     *
     * @return Pair(生成した SSTableReader, 出力件数)
     */
    private fun mergeSstables(src: List<SSTableReader>, out: Path): Pair<SSTableReader, Int> {
        val heap = PriorityQueue<Entry> { a, b ->
            val kc = a.rec.key.compareTo(b.rec.key)
            if (kc != 0) kc else b.rec.seqNo.compareTo(a.rec.seqNo)
        }

        val iters = src.map { it.iterator() }
        for (i in iters.indices) if (iters[i].hasNext()) heap += Entry(iters[i].next(), i)

        val outRecords = ArrayList<Record>(1024)

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

            if (!best.isTombstone) outRecords += best
        }

        SSTableWriter(out, pool).use { it.write(outRecords) }
        val reader = SSTableReader(out, pool)
        return reader to outRecords.size
    }
}
