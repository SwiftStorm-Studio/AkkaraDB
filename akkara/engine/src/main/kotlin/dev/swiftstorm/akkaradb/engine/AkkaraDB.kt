package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.borrow
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
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

class AkkaraDB private constructor(
    private val memTable: MemTable,
    private val packer: AkkBlockPackerDirect,
    private val stripeWriter: AkkStripeWriter,
    private val manifest: AkkManifest,
    private val wal: WalWriter,
    private val pool: BufferPool = Pools.io(),
    private val level0: ConcurrentLinkedDeque<SSTableReader>
) : AutoCloseable {

    private val lock = ReentrantReadWriteLock()
    private val recordWriter = AkkRecordWriter

    /* ---------------- public API ---------------- */

    fun get(key: ByteBuffer): ByteBuffer? = lock.read {
        memTable.get(key)?.value ?: run {
            level0.firstNotNullOfOrNull { sst ->
                if (!sst.mightContain(key)) null
                else sst.get(key)?.value
            } ?: run {
                // 最近の Stripe をスキャン（コスト高, TODO: 最適化）
                AkkStripeReader(
                    stripeWriter.baseDir,
                    stripeWriter.k,
                    stripeWriter.parityCoder
                ).use { reader ->
                    reader.searchLatestStripe(key)?.value
                }
            }
        }
    }

    fun put(rec: Record) = lock.write {
        pool.borrow(recordWriter.computeMaxSize(rec)) { buf ->
            recordWriter.write(rec, buf)
            buf.flip()

            wal.append(buf.duplicate())
            memTable.put(rec)
            packer.addRecord(buf)
        }
    }

    fun flush() = lock.write {
        memTable.flush()
        packer.flush()

        val stripesAfter = try {
            stripeWriter.flush()
        } finally {
            manifest.advance(stripeWriter.stripesWritten)
        }

        wal.checkpoint(stripesAfter, memTable.lastSeq())
        wal.sealSegment()
    }

    override fun close() {
        flush()
        wal.close()
        stripeWriter.close()
        level0.forEach(SSTableReader::close)
    }

    /* --------------- factory -------------------- */

    companion object {
        fun open(
            baseDir: Path,
            k: Int = 4,
            parityCoder: ParityCoder = XorParityCoder(),
            flushThresholdBytes: Long = 64L * 1024 * 1024
        ): AkkaraDB {
            /* ---------- persistent components ---------- */
            val manifest = AkkManifest(baseDir.resolve("manifest.json"))
            val stripe = AkkStripeWriter(baseDir, k, parityCoder, autoFlush = true)
            val wal = WalWriter(baseDir.resolve("wal.log"))
            val pool = Pools.io()
            val level0 = ConcurrentLinkedDeque<SSTableReader>()

            /* ---------- pipeline components ---------- */
            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })

            val mem = MemTable(flushThresholdBytes) { records ->
                if (records.isNotEmpty()) {
                    val sstPath = baseDir.resolve("sst_${System.nanoTime()}.aksst")
                    SSTableWriter(sstPath, pool).use { it.write(records.sortedBy { it.key }) }
                    level0.addFirst(SSTableReader(sstPath, pool))
                }

                for (r in records) {
                    val buf = pool.get(AkkRecordWriter.computeMaxSize(r))
                    AkkRecordWriter.write(r, buf); buf.flip()
                    packer.addRecord(buf); pool.release(buf)
                }
                packer.flush()
            }

            /* ---------- startup recovery ---------- */
            manifest.load()
            stripe.seek(manifest.stripesWritten)

            replayWal(baseDir.resolve("wal.log"), mem)       // durable-before-crash

            AkkStripeReader(baseDir, k, parityCoder).use { reader ->
                var sid = manifest.stripesWritten
                val recReader = AkkRecordReader
                while (true) {
                    val payloads = reader.readStripe() ?: break
                    for (p in payloads) {
                        val dup = p.duplicate()
                        while (dup.hasRemaining()) mem.put(recReader.read(dup))
                    }
                    sid++
                }
            }

            /* ---------- return Ready DB ---------- */
            return AkkaraDB(mem, packer, stripe, manifest, wal, pool, level0)
        }
    }
}
