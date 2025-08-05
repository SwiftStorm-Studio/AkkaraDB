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
     * Returns the value for the given key, or `null` if not found.
     *
     * <h3>Read‑path</h3>
     * 1. MemTable / SST hit (in L0 or L1+)
     * 2. Stripe cache hit
     * 3. Stripe scan fallback (expensive, but only if not in cache)
     */
    fun get(key: ByteBuffer): ByteBuffer? {
        // 1. MemTable hit
        memTable.get(key)?.value?.let { v ->
            return if (v.remaining() == 0) null
            else v
        }

        // 2. SSTables in levels
        for (deque in levels) {
            deque.firstNotNullOfOrNull { sst ->
                if (!sst.mightContain(key)) null
                else sst.get(key)?.value?.let { sv ->
                    if (sv.remaining() != 0) {
                        memTable.put(Record(key.duplicate().rewind(), sv.duplicate().rewind(), memTable.nextSeq()))
                        return sv
                    } else {
                        memTable.put(Record(key.duplicate().rewind(), sv.duplicate().rewind(), memTable.nextSeq()))
                        return null
                    }
                }
            }?.let { return it }
        }

        // 3. Stripe cache
        stripeCache.find(key)?.let { v ->
            if (v.remaining() != 0) {
                memTable.put(Record(key.duplicate().rewind(), v.duplicate().rewind(), memTable.nextSeq()))
                return v
            } else {
                memTable.put(Record(key.duplicate().rewind(), v.duplicate().rewind(), memTable.nextSeq()))
                return null
            }
        }

        // 4. Stripe scan fallback
        AkkStripeReader(stripeWriter.baseDir, stripeWriter.k, stripeWriter.parityCoder).use { reader ->
            val hit = reader.searchLatestStripe(key, manifest.stripesWritten - 1)
            if (hit != null) {
                stripeCache.put(hit.stripeId, hit.blocks)
                val v = hit.record.value
                if (v.remaining() != 0) {
                    memTable.put(Record(key.duplicate().rewind(), v.duplicate().rewind(), memTable.nextSeq()))
                    return v
                } else {
                    memTable.put(Record(key.duplicate().rewind(), v.duplicate().rewind(), memTable.nextSeq()))
                    return null
                }
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
        memTable.flush()
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

            val mem = MemTable(
                flushThresholdBytes,

                onFlush = { records ->
                    if (records.isNotEmpty()) {
                        val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")
                        SSTableWriter(sstPath, pool).use { it.write(records.sortedBy { it.key }) }
                        levels[0].addFirst(SSTableReader(sstPath, pool))
                    }
                    val kStripe = k
                    var i = 0
                    while (i + kStripe <= records.size) {
                        val batch = records.subList(i, i + kStripe)
                        for (r in batch) {
                            val need = AkkRecordWriter.computeMaxSize(r)
                            pool.borrow(need) { tmp ->
                                AkkRecordWriter.write(r, tmp)
                                tmp.flip()
                                val recView = tmp.slice()
                                packer.addRecord(recView)
                            }
                        }
                        packer.flush()
                        i += kStripe
                    }
                }, onEvict = { records ->

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
