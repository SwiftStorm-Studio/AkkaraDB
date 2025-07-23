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
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.engine.wal.replayWal
import dev.swiftstorm.akkaradb.format.akk.*
import dev.swiftstorm.akkaradb.format.akk.parity.XorParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import java.nio.file.Path
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
    private val packer: AkkBlockPackerDirect,
    private val stripeWriter: AkkStripeWriter,
    private val manifest: AkkManifest,
    private val wal: WalWriter,
    private val levels: MutableList<ConcurrentLinkedDeque<SSTableReader>>,
    private val pool: BufferPool = Pools.io(),
    private val compactor: Compactor
) : AutoCloseable {

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
     * Single‑key lookup with three‑tier resolution:
     * <ol>
     *     <li>MemTable (lock‑free)</li>
     *     <li>Level‑0 SSTables (bloom → index → block scan)</li>
     *     <li>Latest stripes (append‑only segment) – heavy fallback</li>
     * </ol>
     *
     * The global [lock] is <strong>only</strong> held for the fast in‑memory
     * tiers. Stripe scanning as well as cache population is executed <em>outside</em>
     * of the critical section, greatly reducing read/write contention.
     */
    fun get(key: ByteBuffer): ByteBuffer? {
        // 1) MemTable / SST hit
        lock.read {
            memTable.get(key)?.value ?: run {
                for (deque in levels) {
                    deque.firstNotNullOfOrNull { sst ->
                        if (!sst.mightContain(key)) null else sst.get(key)?.value
                    }?.let { return it }
                }
                null
            }
        }?.let { return it }

        // 2) Stripe cache
        stripeCache.find(key)?.let { return it }

        // 3) Stripe scan fallback
        AkkStripeReader(stripeWriter.baseDir, stripeWriter.k, stripeWriter.parityCoder).use { reader ->
            val hit = reader.searchLatestStripe(key, manifest.stripesWritten - 1)
            if (hit != null) {
                stripeCache.put(hit.stripeId, hit.blocks)
                return hit.record.value
            }
        }
        return null
    }

    /* ───────── write‑path ───────── */
    fun put(rec: Record) = lock.write {
        pool.borrow(recordWriter.computeMaxSize(rec)) { buf ->
            recordWriter.write(rec, buf); buf.flip()
            wal.append(buf.duplicate())
            memTable.put(rec)
            packer.addRecord(buf)
        }
    }

    fun flush() = lock.write {
        memTable.flush()
        packer.flush()
        stripeWriter.flush()
        manifest.advance(stripeWriter.stripesWritten)
        wal.sealSegment()
        wal.checkpoint(stripeWriter.stripesWritten, memTable.lastSeq())
        compactor.maybeCompact()
    }

    override fun close() {
        flush(); wal.close(); stripeWriter.close()
        levels.forEach { deque -> deque.forEach(SSTableReader::close) }
    }

    /* --------------- factory --------------- */
    companion object {
        fun open(
            baseDir: Path,
            k: Int = 4,
            parityCoder: ParityCoder = XorParityCoder(),
            flushThresholdBytes: Long = 64L * 1024 * 1024,
        ): AkkaraDB {
            val manifest = AkkManifest(baseDir.resolve("manifest.json"))
            val stripe = AkkStripeWriter(baseDir, k, parityCoder, autoFlush = true)
            val wal = WalWriter(baseDir.resolve("wal.log"))
            val pool = Pools.io()

            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })
            val levels = ArrayList<ConcurrentLinkedDeque<SSTableReader>>().apply {
                add(ConcurrentLinkedDeque()) // L0
            }

            val mem = MemTable(flushThresholdBytes, { records ->
                if (records.isNotEmpty()) {
                    val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")
                    SSTableWriter(sstPath, pool).use { it.write(records.sortedBy { it.key }) }
                    levels[0].addFirst(SSTableReader(sstPath, pool))
                }
                // fan‑out to stripe
                for (r in records) {
                    val need = AkkRecordWriter.computeMaxSize(r)
                    println("---- key=${r.key.remaining()}, value=${r.value.remaining()}, need=$need")
                    pool.borrow(need) { tmp ->
                        val written = AkkRecordWriter.write(r, tmp)
                        println("    actually written = $written, buffer cap=${tmp.capacity()}, rem=${tmp.remaining()}")
                        tmp.flip()
                        val recView = tmp.slice()
                        packer.addRecord(recView)
                    }
                }
                packer.flush()
            })

            /* ---- startup recovery ---- */
            manifest.load(); stripe.seek(manifest.stripesWritten)
            replayWal(baseDir.resolve("wal.log"), mem)

            AkkStripeReader(baseDir, k, parityCoder).use { reader ->
                val rr = AkkRecordReader
                repeat(manifest.stripesWritten.toInt()) { sid ->
                    val payloads = reader.readStripe() ?: return@repeat
                    payloads.forEach { p ->
                        val dup = p.duplicate(); while (dup.hasRemaining()) mem.put(rr.read(dup))
                    }
                }
            }

            val compactor = Compactor(levels, baseDir, pool = pool).also { compactor ->
                compactor.maybeCompact()
            }

            return AkkaraDB(mem, packer, stripe, manifest, wal, levels, pool, compactor)
        }
    }
}
