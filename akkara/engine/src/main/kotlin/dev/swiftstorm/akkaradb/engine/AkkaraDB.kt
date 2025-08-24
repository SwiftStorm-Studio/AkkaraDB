package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.sstable.Compactor
import dev.swiftstorm.akkaradb.engine.sstable.SSTableReader
import dev.swiftstorm.akkaradb.engine.sstable.SSTableWriter
import dev.swiftstorm.akkaradb.engine.util.ReaderPool
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.engine.wal.replayWal
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPackerDirect
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import dev.swiftstorm.akkaradb.format.akk.AkkStripeReader
import dev.swiftstorm.akkaradb.format.akk.AkkStripeWriter
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.ReentrantReadWriteLock
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
    private fun metaGet(k: ByteBuffer): KeyMeta? = metaCache[ByteBufferKey.of(k)]

    @Synchronized
    private fun metaPut(k: ByteBuffer, m: KeyMeta) {
        metaCache[ByteBufferKey.of(k)] = m.copy(
            value = m.value.duplicate().apply { rewind() }.asReadOnlyBuffer()
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
    fun getV(key: ByteBuffer): ByteBuffer? =
        getRecordLatest(key)?.let { rec ->
            val v = rec.value
            if (v.remaining() == 0 || rec.isTombstone) null else v
        }

    fun get(key: ByteBuffer): Record? = getRecordLatest(key)

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
        var lastKey: ByteBuffer? = null

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
        val kStore = rec.key.duplicate().apply { rewind() }.asReadOnlyBuffer()
        val vStore = rec.value.duplicate().apply { rewind() }.asReadOnlyBuffer()
        val recStore = Record(kStore, vStore, rec.seqNo, flags = rec.flags)

        val kWal = kStore.duplicate().apply { rewind() }.asReadOnlyBuffer()
        val vWal = vStore.duplicate().apply { rewind() }.asReadOnlyBuffer()
        val recWal = Record(kWal, vWal, rec.seqNo, flags = rec.flags)

        pool.borrow(recordWriter.computeMaxSize(recWal)) { buf ->
            recordWriter.write(recWal, buf)
            buf.flip()
            wal.append(buf.duplicate(), wal.forceDurable.get())
        }

        val accepted = lock.write { memTable.put(recStore) }

        if (accepted) {
            val k = kStore.duplicate().apply { rewind() }
            val v = vStore.duplicate().apply { rewind() }.asReadOnlyBuffer()
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
        stripeWriter.flush()
        wal.checkpoint(stripeWriter.stripesWritten, memTable.lastSeq())
        memTable.flush()
        compactor.maybeCompact()
        manifest.checkpoint(
            name = "flush",
            stripe = stripeWriter.stripesWritten,
            lastSeq = memTable.lastSeq()
        )
        wal.truncate()
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
            // 64 * 1024 * 1024 = 64 MiB
            flushThresholdBytes: Long = 64L * 1024 * 1024,
            walPath: Path = baseDir.resolve("wal.log"),
            walGroupCommitN: Int = 32,
            walGroupCommitMicros: Long = 500,
            walInitCap: Int = 32 * 1024,
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
            val wal = WalWriter(
                path = walPath,
                groupCommitN = walGroupCommitN,
                groupCommitMicros = walGroupCommitMicros,
                initCap = walInitCap
            )
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

                    val sorted = records.sortedWith { a, b -> cmp(a.key, b.key) }

                    val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")
                    SSTableWriter(sstPath, pool).use { it.write(sorted) }

                    val firstKey = sorted.firstOrNull()?.key?.base64()
                    val lastKey = sorted.lastOrNull()?.key?.base64()
                    manifest.sstSeal(
                        level = 0,
                        file = sstPath.fileName.toString(),
                        entries = sorted.size.toLong(),
                        firstKeyHex = firstKey,
                        lastKeyHex = lastKey
                    )
                    levels[0].addFirst(SSTableReader(sstPath, pool))

                    val kStripe = k
                    if (sorted.size >= kStripe) {
                        var i = 0
                        while (i + kStripe <= sorted.size) {
                            val batch = sorted.subList(i, i + kStripe)
                            for (r in batch) {
                                val need = AkkRecordWriter.computeMaxSize(r)
                                pool.borrow(need) { tmp ->
                                    AkkRecordWriter.write(r, tmp)
                                    tmp.flip()
                                    packer.addRecord(tmp.slice())
                                }
                            }
                            packer.flush()
                            i += kStripe
                        }
                    }

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
            replayWal(
                walPath,
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

        fun ByteBuffer.base64(): String {
            val dup = duplicate().apply { rewind() }
            val arr = ByteArray(dup.remaining())
            dup.get(arr)
            return BASE64_TL.get().encodeToString(arr)
        }
    }

    private fun startsWith(key: ByteBuffer, prefix: ByteBuffer): Boolean {
        val k = BufView(key)
        val p = BufView(prefix)

        val pRem = p.rem()
        if (pRem == 0) return true
        if (k.rem() < pRem) return false

        // Fast path: 両方ヒープ（配列持ち）なら Arrays.mismatch で一発比較（JDK 9+）
        if (key.hasArray() && prefix.hasArray()) {
            val ka = key.array()
            val pa = prefix.array()
            val kOff = key.arrayOffset() + k.pos
            val pOff = prefix.arrayOffset() + p.pos
            // mismatch(...) は -1 を返すと「差異なし」
            return java.util.Arrays.mismatch(ka, kOff, kOff + pRem, pa, pOff, pOff + pRem) == -1
        }

        // Fallback: どの ByteBuffer でも動く絶対 index 比較
        // （符号は equality には無関係なので &0xFF は不要）
        var i = 0
        val kPos = k.pos
        val pPos = p.pos
        while (i < pRem) {
            if (key.get(kPos + i) != prefix.get(pPos + i)) return false
            i++
        }
        return true
    }

    @JvmInline
    private value class BufView(val buf: ByteBuffer) {
        val pos: Int get() = buf.position()
        val lim: Int get() = buf.limit()
        fun rem(): Int = lim - pos
    }

    data class ScanOptions(
        val from: ByteBuffer? = null,
        val toExclusive: ByteBuffer? = null,
        val prefix: ByteBuffer? = null,
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

        fun peekKey(): ByteBuffer = next!!.key.duplicate().apply { rewind() }
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
        val value: ByteBuffer
    )

    private data class ByteBufferKey(val bb: ByteBuffer, private val hash: Int) {
        override fun hashCode(): Int = hash
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return other is ByteBufferKey && cmp(bb, other.bb) == 0
        }

        companion object {
            fun of(src: ByteBuffer): ByteBufferKey {
                val dup = src.duplicate().apply { rewind() }.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)
                var h = 1
                while (dup.hasRemaining()) h = 31 * h + (dup.get().toInt() and 0xFF)
                dup.rewind()
                return ByteBufferKey(dup, h)
            }
        }
    }

    private fun getRecordLatest(key: ByteBuffer): Record? {
        val k = key.duplicate().apply { rewind() }.asReadOnlyBuffer()

        memTable.get(k)?.let { rec ->
            val vv = rec.value.duplicate().apply { rewind() }.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)
            metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, vv))
            return rec
        }

        metaGet(k)?.let { return Record(k, it.value.duplicate().apply { rewind() }.order(ByteOrder.LITTLE_ENDIAN), it.seqNo) }

        for (deque in levels) {
            val rec = deque.firstNotNullOfOrNull { sst ->
                if (!sst.mightContain(k)) null else sst.get(k)
            }
            if (rec != null) {
                val vv = rec.value.duplicate().apply { rewind() }.asReadOnlyBuffer()
                metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, vv))
                return rec
            }
        }

        readerPool.withResource { reader ->
            val lastStripe = manifest.stripesWritten - 1
            if (lastStripe >= 0) {
                val hit = reader.searchLatestStripe(k, lastStripe) ?: return@withResource null
                val rec = hit.record
                val vv = rec.value.duplicate().apply { rewind() }.asReadOnlyBuffer()
                metaPut(k, KeyMeta(rec.seqNo, rec.isTombstone, vv))
                return rec
            }
        }

        return null
    }
}
