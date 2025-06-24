package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.engine.wal.replayWal
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPackerDirect
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import dev.swiftstorm.akkaradb.format.akk.AkkRecordWriter
import dev.swiftstorm.akkaradb.format.akk.AkkStripeReader
import dev.swiftstorm.akkaradb.format.akk.AkkStripeWriter
import dev.swiftstorm.akkaradb.format.akk.parity.XorParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write

class AkkaraDB private constructor(
    private val memTable: MemTable,
    private val packer: AkkBlockPackerDirect,
    private val stripeWriter: AkkStripeWriter,
    private val manifest: AkkManifest,
    private val wal: WalWriter,
    private val pool: BufferPool = Pools.io()
) : AutoCloseable {

    private val lock = ReentrantReadWriteLock()
    private val recordWriter = AkkRecordWriter

    /* ---------------- public API ---------------- */

    fun put(rec: Record) = lock.write {
        // 1) Encode record using pooled buffer
        val buf = pool.get(recordWriter.computeMaxSize(rec))
        recordWriter.write(rec, buf)
        buf.flip()

        // 2) WAL first
        wal.append(buf.duplicate())

        // 3) MemTable
        memTable.put(rec)

        // 4) BlockPacker
        packer.addRecord(buf)
        pool.release(buf)
    }

    fun flush() = lock.write {
        // Drain MemTable -> packer
        memTable.flush()
        packer.flush()

        // Stripe flush with manifest advance
        val stripesAfter = try {
            stripeWriter.flush()
        } finally {
            manifest.advance(stripeWriter.stripesWritten)
        }

        // WAL checkpoint & seal
        wal.checkpoint(stripesAfter, memTable.lastSeq())
        wal.sealSegment()
    }

    override fun close() {
        flush()
        wal.close()
        stripeWriter.close()
    }

    /* --------------- factory -------------------- */

    companion object {
        fun open(
            baseDir: Path,
            k: Int = 4,
            parityCoder: ParityCoder = XorParityCoder(),
            flushThresholdBytes: Long = 64L * 1024 * 1024
        ): AkkaraDB {
            /* 1. persistent components */
            val manifest = AkkManifest(baseDir.resolve("manifest.json"))
            val stripe = AkkStripeWriter(baseDir, k, parityCoder, autoFlush = true)
            val wal = WalWriter(baseDir.resolve("wal.log"))
            val writer = AkkRecordWriter
            val pool = Pools.io()

            /* 2. pipeline components */
            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })

            val mem = MemTable(flushThresholdBytes) { records ->
                for (r in records) {
                    val buf = pool.get(writer.computeMaxSize(r))
                    writer.write(r, buf)
                    buf.flip()
                    packer.addRecord(buf)
                    pool.release(buf)
                }
                packer.flush()
            }

            /* 3. Load manifest & seek StripeWriter */
            manifest.load()
            stripe.seek(manifest.stripesWritten)

            /* 4. WAL replay (durable before crash) */
            replayWal(baseDir.resolve("wal.log"), mem)

            /* 5. Stripe rescan (data already on disk) */
            val stripeReader = AkkStripeReader(baseDir, k, parityCoder)
            val recReader = AkkRecordReader
            var sid = manifest.stripesWritten
            while (true) {
                val payloads = stripeReader.readStripe() ?: break
                for (p in payloads) {
                    val dup = p.duplicate()
                    while (dup.hasRemaining()) mem.put(recReader.read(dup))
                }
                sid++
            }
            stripeReader.close()

            /* 6. return ready instance */
            return AkkaraDB(mem, packer, stripe, manifest, wal, pool)
        }
    }
}
