@file:Suppress("ReplaceCallWithBinaryOperator")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.sstable.Compactor
import dev.swiftstorm.akkaradb.engine.sstable.SSTableReader
import dev.swiftstorm.akkaradb.engine.sstable.SSTableWriter
import dev.swiftstorm.akkaradb.engine.util.ReaderPool
import dev.swiftstorm.akkaradb.engine.wal.WalReplay
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPackerDirect
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import dev.swiftstorm.akkaradb.format.akk.AkkStripeReader
import dev.swiftstorm.akkaradb.format.akk.AkkStripeWriter
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.system.measureNanoTime

/**
 * Main entry‑point of AkkaraDB.
 *
 * <h3>Optimisations (2025‑07)</h3>
 * 5. <b>Stripe LRU cache</b> – recent <em>k</em> stripes are retained in‑memory
 *    as a compact [IndexBlock] of decoded records.<br/>
 * 6. <b>Read‑fast‑path</b> – the global [lock] is <em>not</em> held during the
 *    expensive stripe‑scan fallback, unblocking concurrent writes.
 */
class AkkaraDB private constructor(
    private val memTable: MemTable,
    private val stripeWriter: AkkStripeWriter,
    private val manifest: AkkManifest,
    val wal: WalWriter,
    private val levels: MutableList<ConcurrentLinkedDeque<SSTableReader>>,
    private val pool: BufferPool = Pools.io(),
    private val compactor: Compactor,
    private val metaCacheCap: Int = 1024,
) : AutoCloseable {

    private val metaCache = object : LinkedHashMap<ByteBufferKey, KeyMeta>(metaCacheCap, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<ByteBufferKey, KeyMeta>) =
            size > metaCacheCap
    }

    private val readerPool = ReaderPool(
        factory = { AkkStripeReader(stripeWriter.baseDir, stripeWriter.k, stripeWriter.parityCoder) },
        max = Runtime.getRuntime().availableProcessors().coerceAtMost(8)
    )

    @Synchronized
    private fun metaGet(k: ByteBufferL): KeyMeta? = metaCache[ByteBufferKey.of(k)]

    @Synchronized
    private fun metaPut(k: ByteBufferL, m: KeyMeta) {
        metaCache[ByteBufferKey.of(k)] = m.copy(
            value = m.value.duplicate().apply { rewind() }.asReadOnly()
        )
    }

    /* ───────── synchronisation ───────── */
    private val lock = ReentrantReadWriteLock()
    /* ───────── public API ───────── */

    /**
     * Returns the value for the given key, or `null` if not found.
     *
     * Read‑path:
     * 1. MemTable / HotReadCache hit
     * 2. SST hit (L0 → L1+)
     * 3. Stripe cache hit
     * 4. Stripe scan fallback
     */
    fun getV(key: ByteBufferL): ByteBufferL? =
        getRecordLatest(key)?.let { rec ->
            val v = rec.value
            if (v.remaining == 0 || rec.isTombstone) null else v
        }

    fun get(key: ByteBufferL): Record? = getRecordLatest(key)

    fun scanRange(
        startKeyInclusive: ByteBufferL,
        endKeyExclusive: ByteBufferL? = null,
        limit: Long? = null
    ): Sequence<Record> = sequence {
        val memSnapshot = memTable.getAll()
            .asSequence()
            .filter { r ->
                val geStart = r.key.compareTo(startKeyInclusive) >= 0
                val ltEnd = endKeyExclusive?.let { r.key.compareTo(it) < 0 } ?: true
                geStart && ltEnd
            }
            .sortedWith { a, b -> a.key.compareTo(b.key) } // unsigned lex
            .iterator()

        val iters = ArrayList<Iterator<Record>>()
        iters += memSnapshot
        lock.read {
            for (dq in levels) for (sst in dq) {
                iters += sst.scanRange(startKeyInclusive, endKeyExclusive)
            }
        }

        data class Entry(val rec: Record, val src: Int)

        val heap = PriorityQueue<Entry> { a, b ->
            val kc = a.rec.key.compareTo(b.rec.key)
            if (kc != 0) kc else b.rec.seqNo.compareTo(a.rec.seqNo)
        }

        val heads = ArrayList<Record?>(iters.size).apply { repeat(iters.size) { add(null) } }
        for (i in iters.indices) if (iters[i].hasNext()) {
            val r = iters[i].next()
            heads[i] = r
            heap += Entry(r, i)
        }

        var emitted = 0L
        while (heap.isNotEmpty()) {
            val first = heap.poll()
            val curKey = first.rec.key

            var best = first.rec
            fun pushNextFromSrc(src: Int) {
                if (iters[src].hasNext()) {
                    val nr = iters[src].next()
                    heads[src] = nr
                    heap += Entry(nr, src)
                } else {
                    heads[src] = null
                }
            }
            pushNextFromSrc(first.src)

            while (heap.isNotEmpty() && heap.peek().rec.key.compareTo(curKey) == 0) {
                val e = heap.poll()
                if (e.rec.seqNo > best.seqNo) best = e.rec
                pushNextFromSrc(e.src)
            }

            if (!best.isTombstone) {
                yield(best)
                emitted++
                if (limit != null && emitted >= limit) return@sequence
            }
        }
    }

    /* ───────── write‑path ───────── */
    fun put(rec: Record) {
        val k = rec.key.duplicate()
        val v = rec.value.duplicate()

        pool.borrow(AkkRecordWriter.computeMaxSize(rec)) { tmp ->
            AkkRecordWriter.write(rec, tmp)
            tmp.flip()
            wal.append(tmp)
        }

        val accepted = lock.write { memTable.put(rec) }

        if (accepted) {
            metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, v))
        }
    }

    /**
     * Puts the record into MemTable without writing to WAL.
     * Use with caution: this method is not crash-safe.
     * For debugging and testing only.
     */
    @PublishedApi
    @Deprecated("unsafe: does not write to WAL", level = DeprecationLevel.WARNING)
    internal fun putUnsafeNoWal(rec: Record) {
        val kStore = rec.key.duplicate().apply { rewind() }.asReadOnly()
        val vStore = rec.value.duplicate().apply { rewind() }.asReadOnly()
        val recStore = Record(kStore, vStore, rec.seqNo, flags = rec.flags)

        val accepted = lock.write { memTable.put(recStore) }
        if (accepted) {
            val k = kStore.duplicate().apply { rewind() }
            val v = vStore.duplicate().apply { rewind() }.asReadOnly()
            metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, v))
        }
    }

    /**
     * Flushes MemTable to SSTable, seals current WAL segment,
     * checkpoints StripeWriter and Manifest, and maybe compacts SSTables.
     *
     * This method acquires the global write‑lock, blocking all other
     * operations until it completes.
     */
    fun flush() = lock.write {
        val t0 = System.nanoTime()

        val tSeal = measureNanoTime { wal.sealSegment() }
        val tMemFlush = measureNanoTime { memTable.flush() }
        val tStripe = measureNanoTime { stripeWriter.flush() }

        val lastSeq = memTable.lastSeq()
        val stripes = stripeWriter.stripesWritten

        val tManifest = measureNanoTime { manifest.checkpoint("flush", stripes, lastSeq) }
        val tWalCkpt = measureNanoTime { wal.checkpoint(stripes, lastSeq) }

        val tCompact = measureNanoTime { compactor.maybeCompact() }
        val tPrune = measureNanoTime { wal.pruneObsoleteSegments() }

        val total = System.nanoTime() - t0

        fun Long.us(): Double = this / 1_000.0

        println(
            "flush timings [µs] " +
                    "seal=${tSeal.us()}, " +
                    "memFlush=${tMemFlush.us()}, " +
                    "stripeFlush=${tStripe.us()}, " +
                    "manifest=${tManifest.us()}, " +
                    "walCkpt=${tWalCkpt.us()}, " +
                    "compact=${tCompact.us()}, " +
                    "prune=${tPrune.us()}, " +
                    "total=${total.us()}"
        )
    }

    fun lastSeq(): Long = memTable.lastSeq()

    fun nextSeq(): Long = memTable.nextSeq()

    override fun close() {
        flush()
        memTable.close(); wal.close(); stripeWriter.close(); readerPool.close()
        levels.forEach { deque -> deque.forEach(SSTableReader::close) }
    }

    /* --------------- factory --------------- */
    companion object {
        private val BASE64_TL = ThreadLocal.withInitial {
            Base64.getEncoder()
        }

        fun open(
            baseDir: Path,
            stripeK: Int,
            stripeAutoFlush: Boolean,
            parityCoder: ParityCoder,
            flushThresholdBytes: Long,
            walDir: Path,
            walFilePrefix: String,
            walEnableLog: Boolean,
            walFastMode: Boolean,
            walFsyncBatchN: Int?,
            walFsyncIntervalMicros: Long?, //4_000L 4000 micros = 4 ms
            walQueueCapacity: Int,
            walBackoffNanos: Long,
            metaCacheCap: Int,
        ): AkkaraDB {
            val manifest = AkkManifest(baseDir.resolve("manifest.json"))
            manifest.load()
            val stripe = AkkStripeWriter(
                baseDir,
                stripeK,
                parityCoder,
                autoFlush = stripeAutoFlush,
                onCommit = { committed -> manifest.advance(committed) }
            )
            val wal = WalWriter(
                dir = walDir,
                filePrefix = walFilePrefix,
                enableLog = walEnableLog,
                fastMode = walFastMode,
                fsyncBatchN = walFsyncBatchN,
                fsyncIntervalMicros = walFsyncIntervalMicros,
                queueCapacity = walQueueCapacity,
                backoffNanos = walBackoffNanos
            )
            val pool = Pools.io()
            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })

            val levels = buildLevelsFromManifest(baseDir, manifest, pool)

            if (levels.isEmpty()) levels += ConcurrentLinkedDeque<SSTableReader>()

            val mem = MemTable(
                thresholdBytesPerShard = flushThresholdBytes,
                onFlush = { records ->
                    if (records.isEmpty()) {
                        manifest.checkpoint("memFlush-empty")
                        return@MemTable
                    }

                    records.sortWith { a, b -> cmp(a.key, b.key) }

                    val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")
                    SSTableWriter(sstPath, pool).use { it.write(records) }

                    if (records.size >= stripeK) {
                        for (r in records) {
                            val need = AkkRecordWriter.computeMaxSize(r)
                            pool.borrow(need) { tmp ->
                                AkkRecordWriter.write(r, tmp)
                                tmp.flip()
                                val safe = ByteBufferL.allocate(tmp.remaining)
                                safe.put(tmp); safe.flip()
                                packer.addRecord(safe)
                            }
                        }
                        packer.flush()
                    }

                    val firstKey = records.first().key.base64()
                    val lastKey = records.last().key.base64()
                    manifest.sstSeal(
                        level = 0,
                        file = sstPath.fileName.toString(),
                        entries = records.size.toLong(),
                        firstKeyHex = firstKey,
                        lastKeyHex = lastKey
                    )
                    levels[0].addFirst(SSTableReader(sstPath, pool))

                    manifest.checkpoint("memFlush")

                    checkNotNull(levels[0].first().get(records.first().key)) { "SST get() failed just after flush!" }
                }
            )

            stripe.truncateTo(manifest.stripesWritten)

            val cp = manifest.lastCheckpoint
            WalReplay.replay(
                walDir,
                mem,
                cp?.stripe ?: 0L,
                cp?.lastSeq ?: Long.MIN_VALUE
            )

            val compactor = Compactor(levels, baseDir, manifest, pool = pool).also { it.maybeCompact() }

            return AkkaraDB(mem, stripe, manifest, wal, levels, pool, compactor, metaCacheCap)
        }

        private fun buildLevelsFromManifest(
            baseDir: Path,
            manifest: AkkManifest,
            pool: BufferPool
        ): MutableList<ConcurrentLinkedDeque<SSTableReader>> {
            val maxLv = manifest.sstSeals.maxOfOrNull { it.level } ?: 0
            val lv = MutableList(maxLv + 1) { ConcurrentLinkedDeque<SSTableReader>() }

            for (e in manifest.sstSeals) {
                val p = baseDir.resolve(e.file)
                if (java.nio.file.Files.exists(p)) {
                    lv[e.level].addFirst(SSTableReader(p, pool))
                }
            }
            if (lv[0].isEmpty()) lv[0] = ConcurrentLinkedDeque()
            return lv
        }

        private fun cmp(a: ByteBufferL, b: ByteBufferL): Int = a.compareTo(b)

        fun ByteBufferL.base64(): String {
            val enc = BASE64_TL.get()
            val src = this.duplicate().apply { rewind() }.asReadOnlyByteBuffer()
            val out = enc.encode(src)
            out.flip()
            return StandardCharsets.ISO_8859_1.decode(out).toString()
        }
    }

    private data class KeyMeta(
        val seqNo: Long,
        val isTombstone: Boolean,
        val value: ByteBufferL
    )

    private data class ByteBufferKey(val bb: ByteBufferL, private val hash: Int) {
        override fun hashCode(): Int = hash
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return other is ByteBufferKey && bb.compareTo(other.bb) == 0
        }

        companion object {
            fun of(src: ByteBufferL): ByteBufferKey {
                val dup = src.duplicate().apply { rewind() }.asReadOnly()
                var h = 1
                while (dup.hasRemaining()) h = 31 * h + (dup.get().toInt() and 0xFF)
                dup.rewind()
                return ByteBufferKey(dup, h)
            }
        }
    }

    private fun getRecordLatest(key: ByteBufferL): Record? {
        val k = key.duplicate().apply { rewind() }.asReadOnly()

        memTable.get(k)?.let { rec ->
            val vv = rec.value.duplicate().apply { rewind() }.asReadOnly()
            metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, vv))
            return rec
        }

        metaGet(k)?.let { return Record(k, it.value.duplicate().apply { rewind() }, it.seqNo) }

        lock.read {
            for (deque in levels) {
                val rec = deque.firstNotNullOfOrNull { sst -> sst.get(k) }
                if (rec != null) {
                    val vv = rec.value.duplicate().apply { rewind() }.asReadOnly()
                    metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, vv))
                    return rec
                }
            }
        }

        readerPool.withResource { reader ->
            val lastStripe = manifest.stripesWritten - 1
            if (lastStripe >= 0) {
                val hit = reader.searchLatestStripe(k, lastStripe) ?: return@withResource null
                val rec = hit.record
                val vv = rec.value.duplicate().apply { rewind() }.asReadOnly()
                metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, vv))
                return rec
            }
        }

        return null
    }
}
