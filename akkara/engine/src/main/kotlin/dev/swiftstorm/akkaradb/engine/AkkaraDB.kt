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
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

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
    private val recordWriter = AkkRecordWriter

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

    fun getAll(opts: ScanOptions = ScanOptions()): Sequence<Record> = sequence {
        val its = ArrayList<PeekingIter>()
        // ---- MemTable
        memTable.iterator(opts.from, opts.toExclusive).let { its += PeekingIter(it) }
        // ---- SSTables
        for (level in levels) {
            for (sst in level) {
                its += PeekingIter(sst.iterator(opts.from, opts.toExclusive))
            }
        }

        val heap = PriorityQueue<PeekingIter> { a, b ->
            cmp(a.peekKey(), b.peekKey())
        }
        its.filter { it.hasNext() }.forEach(heap::add)

        var emitted = 0
        var lastKey: ByteBufferL? = null

        while (heap.isNotEmpty()) {
            val head = heap.poll()
            val key = head.peekKey()

            if (opts.prefix != null) {
                val pfx = opts.prefix.duplicate().apply { rewind() }
                val kdup = key.duplicate().apply { rewind() }
                if (!startsWith(kdup, pfx)) {
                    continue
                }
            }

            var best: Record? = null
            fun consider(r: Record) {
                if (best == null || r.seqNo > best!!.seqNo) best = r
            }

            consider(head.next())
            if (head.hasNext()) heap.add(head)

            val tmp = ArrayList<PeekingIter>()
            while (heap.isNotEmpty() && cmp(heap.peek().peekKey(), key) == 0) {
                val it = heap.poll()
                consider(it.next())
                if (it.hasNext()) tmp += it
            }
            tmp.forEach(heap::add)

            val chosen = best!!
            val isTomb = chosen.isTombstone

            if (opts.toExclusive != null && cmp(key, opts.toExclusive) >= 0) break

            if (opts.prefix != null && !startsWith(key.duplicate().apply { rewind() }, opts.prefix.duplicate().apply { rewind() })) {
                continue
            }

            if (!opts.includeTombstone && isTomb) {
                continue
            } else {
                if (lastKey == null || cmp(lastKey, key) != 0) {
                    yield(chosen)
                    emitted++
                    lastKey = key.duplicate().apply { rewind() }
                    if (opts.limit != null && emitted >= opts.limit) break
                }
            }
        }
    }

    /* ───────── write‑path ───────── */
    fun put(rec: Record) {
        val kStore = rec.key.duplicate().apply { rewind() }.asReadOnly()
        val vStore = rec.value.duplicate().apply { rewind() }.asReadOnly()
        val recStore = Record(kStore, vStore, rec.seqNo, flags = rec.flags)

        val kWal = kStore.duplicate().apply { rewind() }.asReadOnly()
        val vWal = vStore.duplicate().apply { rewind() }.asReadOnly()
        val recWal = Record(kWal, vWal, rec.seqNo, flags = rec.flags)

        pool.borrow(recordWriter.computeMaxSize(recWal)) { buf ->
            recordWriter.write(recWal, buf)
            buf.flip()
            wal.append(buf.duplicate())
        }

        val accepted = lock.write { memTable.put(recStore) }

        if (accepted) {
            val k = kStore.duplicate().apply { rewind() }
            val v = vStore.duplicate().apply { rewind() }.asReadOnly()
            metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, v))
        }
    }

    /**
     * Puts the record into MemTable without writing to WAL.
     * Use with caution: this method is not crash-safe.
     * For debugging and testing only.
     */
    @PublishedApi
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
        wal.sealSegment()
        memTable.flush()
        stripeWriter.flush()
        manifest.checkpoint("flush", stripeWriter.stripesWritten, memTable.lastSeq())
        wal.checkpoint(stripeWriter.stripesWritten, memTable.lastSeq())

        compactor.maybeCompact()
        wal.pruneObsoleteSegments()
    }

    fun lastSeq(): Long = memTable.lastSeq()

    fun nextSeq(): Long = memTable.nextSeq()

    override fun close() {
        flush(); wal.close(); stripeWriter.close(); readerPool.close()
        levels.forEach { deque -> deque.forEach(SSTableReader::close) }
    }

    /* --------------- factory --------------- */
    companion object {
        private val BASE64_TL = ThreadLocal.withInitial {
            Base64.getEncoder()
        }

        fun open(
            baseDir: Path,
            k: Int = 4,
            m: Int = 2,
            parityCoder: ParityCoder = RSParityCoder(m),
            // 32 * 1024 * 1024 = 32 MiB
            flushThresholdBytes: Long = 32L * 1024 * 1024,
            walDir: Path = baseDir.resolve("wal"),
            walFilePrefix: String = "wal",
            walEnableLog: Boolean = true,
            metaCacheCap: Int = 1024,
        ): AkkaraDB {
            val manifest = AkkManifest(baseDir.resolve("manifest.json"))
            manifest.load()
            val stripe = AkkStripeWriter(
                baseDir,
                k,
                parityCoder,
                autoFlush = true,
                onCommit = { committed -> manifest.advance(committed) }
            )
            val wal = WalWriter(dir = walDir, filePrefix = walFilePrefix, enableLog = walEnableLog)
            val pool = Pools.io()
            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })

            val levels = buildLevelsFromManifest(baseDir, manifest, pool)

            if (levels.isEmpty()) levels += ConcurrentLinkedDeque<SSTableReader>()

            val mem = MemTable(
                flushThresholdBytes,
                onFlush = { records ->
                    if (records.isEmpty()) {
                        manifest.checkpoint("memFlush-empty")
                        return@MemTable
                    }

                    records.sortWith { a, b -> cmp(a.key, b.key) }

                    val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")
                    SSTableWriter(sstPath, pool).use { it.write(records) }

                    if (records.size >= k) {
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
                },
                onEvict = { records ->
                    for (r in records) {
                        val need = AkkRecordWriter.computeMaxSize(r)
                        pool.borrow(need) { tmp ->
                            AkkRecordWriter.write(r, tmp)
                            tmp.flip()
                            packer.addRecord(tmp.slice())
                        }
                    }
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

    private fun startsWith(key: ByteBufferL, prefix: ByteBufferL): Boolean {
        val k = key.duplicate().apply { rewind() }.asReadOnlyByteBuffer()
        val p = prefix.duplicate().apply { rewind() }.asReadOnlyByteBuffer()
        val n = p.remaining()
        if (n == 0) return true
        if (k.remaining() < n) return false

        val ks = k.duplicate().apply { limit(position() + n) }.slice()
        val ps = p.slice()
        val mm = ks.mismatch(ps)
        return mm == -1
    }

    data class ScanOptions(
        val from: ByteBufferL? = null,
        val toExclusive: ByteBufferL? = null,
        val prefix: ByteBufferL? = null,
        val limit: Int? = null,
        val includeTombstone: Boolean = false
    )

    private class PeekingIter(it: Iterator<Record>) : Iterator<Record> {
        private var next: Record? = null
        private val src = it

        init {
            advance()
        }

        private fun advance() {
            next = if (src.hasNext()) src.next() else null
        }

        fun peekKey(): ByteBufferL = next!!.key.duplicate().apply { rewind() }
        override fun hasNext(): Boolean = next != null
        override fun next(): Record {
            val r = next ?: throw NoSuchElementException()
            advance()
            return r
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
                val rec = deque.firstNotNullOfOrNull { sst ->
                    if (!sst.mightContain(k)) null else sst.get(k)
                }
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
