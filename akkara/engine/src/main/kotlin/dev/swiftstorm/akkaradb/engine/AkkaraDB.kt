package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.engine.cache.HotReadCache
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.sstable.Compactor
import dev.swiftstorm.akkaradb.engine.sstable.SSTableReader
import dev.swiftstorm.akkaradb.engine.sstable.SSTableWriter
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.engine.wal.replayWal
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPackerDirect
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import dev.swiftstorm.akkaradb.format.akk.AkkStripeReader
import dev.swiftstorm.akkaradb.format.akk.AkkStripeWriter
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
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
    private val wal: WalWriter,
    private val levels: MutableList<ConcurrentLinkedDeque<SSTableReader>>,
    private val pool: BufferPool = Pools.io(),
    private val compactor: Compactor
) : AutoCloseable {

    private val readCache = HotReadCache(64L * 1024 * 1024)

    /* ───────── synchronisation ───────── */
    private val lock = ReentrantReadWriteLock()
    private val recordWriter = AkkRecordWriter

    /* ───────── Stripe LRU Cache ───────── */
    /**
     * A simple LRU cache for the latest <em>k</em> stripes, where each stripe
     * is a list of [Record]s. The cache is used to speed up single‑key lookups
     * by avoiding expensive stripe scans.
     */
    private class StripeCache(private val capacity: Int) {
        private val cache = object : LinkedHashMap<Long, List<Record>>(capacity, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Long, List<Record>>): Boolean = size > capacity
        }
        @Synchronized
        fun put(sid: Long, recs: List<Record>) {
            cache[sid] = recs
        }

        @Synchronized
        fun find(key: ByteBuffer): ByteBuffer? = cache.values.firstNotNullOfOrNull { recs ->
            recs.firstOrNull { it.key.compareTo(key) == 0 }?.value
        }
    }
    private val stripeCache = StripeCache(capacity = 8)

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
    fun get(key: ByteBuffer): ByteBuffer? {
        fun roDup(bb: ByteBuffer) = bb.duplicate().apply { rewind() }

        val k = roDup(key)

        memTable.get(k)?.value?.let { v ->
            return if (v.remaining() == 0) null else v
        }

        readCache.get(k)?.let { v ->
            return if (v.remaining() == 0) null else v
        }

        // 2. SSTables（Bloom → Index → data）
        for (deque in levels) {
            deque.firstNotNullOfOrNull { sst ->
                if (!sst.mightContain(k)) return@firstNotNullOfOrNull null
                val rec = sst.get(k) ?: return@firstNotNullOfOrNull null
                val sv = rec.value
                readCache.put(k, sv)
                return@firstNotNullOfOrNull if (sv.remaining() != 0) sv else null
            }?.let { return it }
        }

        // 3. Stripe cache
        stripeCache.find(k)?.let { v ->
            readCache.put(k, v)
            return if (v.remaining() != 0) v else null
        }

        // 4. Stripe scan fallback (expensive)
        AkkStripeReader(stripeWriter.baseDir, stripeWriter.k, stripeWriter.parityCoder).use { reader ->
            val hit = reader.searchLatestStripe(k, manifest.stripesWritten - 1)
            if (hit != null) {
                stripeCache.put(hit.stripeId, hit.blocks)
                val v = hit.record.value
                readCache.put(k, v)
                return if (v.remaining() != 0) v else null
            }
        }

        return null
    }

    /* ───────── write‑path ───────── */
    fun put(rec: Record) = lock.write {
        pool.borrow(recordWriter.computeMaxSize(rec)) { buf ->
            recordWriter.write(rec, buf)
            buf.flip()
            wal.append(buf.duplicate())
        }
        memTable.put(rec)
    }

    fun flush() = lock.write {
        wal.sealSegment()
        wal.checkpoint(stripeWriter.stripesWritten, memTable.lastSeq())
        memTable.flush()
        compactor.maybeCompact()
        manifest.checkpoint(
            name = "flush",
            stripe = stripeWriter.stripesWritten,
            lastSeq = memTable.lastSeq()
        )
    }

    fun lastSeq(): Long = memTable.lastSeq()

    override fun close() {
        flush(); wal.close(); stripeWriter.close()
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
        ): AkkaraDB {
            val manifest = AkkManifest(baseDir.resolve("manifest.json"))
            val stripe = AkkStripeWriter(
                baseDir,
                k,
                parityCoder,
                autoFlush = true,
                onCommit = { committed -> manifest.advance(committed) }
            )
            val wal = WalWriter(baseDir.resolve("wal.log"))
            val pool = Pools.io()
            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })

            val levels = buildLevelsFromManifest(baseDir, manifest, pool)

            if (levels.isEmpty()) levels += ConcurrentLinkedDeque<SSTableReader>()

            val mem = MemTable(
                flushThresholdBytes,
                onFlush = { records ->
                    if (records.isNotEmpty()) {
                        val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")

                        val sorted = records.sortedWith { r1, r2 -> cmp(r1.key, r2.key) }
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
                    }

                    val kStripe = k
                    if (records.size >= kStripe) {
                        var i = 0
                        while (i + kStripe <= records.size) {
                            val batch = records.subList(i, i + kStripe)
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

            manifest.load()
            stripe.truncateTo(manifest.stripesWritten)

            replayWal(baseDir.resolve("wal.log"), mem)

            val compactor = Compactor(levels, baseDir, manifest, pool = pool).also { it.maybeCompact() }

            return AkkaraDB(mem, stripe, manifest, wal, levels, pool, compactor)
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


        private fun loadExistingSstLevels(
            baseDir: Path,
            pool: BufferPool
        ): MutableList<ConcurrentLinkedDeque<SSTableReader>> {
            val levels = ArrayList<ConcurrentLinkedDeque<SSTableReader>>()
            val l0 = ConcurrentLinkedDeque<SSTableReader>()
            val l1 = ConcurrentLinkedDeque<SSTableReader>()

            java.nio.file.Files.list(baseDir).use { stream ->
                stream.filter { p -> p.fileName.toString().endsWith(".aksst") && p.fileName.toString().startsWith("sst_") }
                    .sorted(Comparator.comparingLong<Path> { p ->
                        java.nio.file.Files.getLastModifiedTime(p).toMillis()
                    }.reversed())
                    .forEach { p -> l0.addFirst(SSTableReader(p, pool)) }
            }

            java.nio.file.Files.list(baseDir).use { stream ->
                stream.filter { p -> p.fileName.toString().endsWith(".aksst") && p.fileName.toString().startsWith("sst_compact_") }
                    .sorted(Comparator.comparingLong<Path> { p ->
                        java.nio.file.Files.getLastModifiedTime(p).toMillis()
                    }.reversed())
                    .forEach { p -> l1.addFirst(SSTableReader(p, pool)) }
            }

            levels += l0
            if (l1.isNotEmpty()) levels += l1
            return levels
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
}
