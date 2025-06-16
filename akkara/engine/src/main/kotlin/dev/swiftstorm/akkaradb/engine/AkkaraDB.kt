package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import dev.swiftstorm.akkaradb.format.api.RecordWriter
import dev.swiftstorm.akkaradb.format.api.StripeWriter
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class AkkaraDB private constructor(
    private val memTable: MemTable,
    private val packer: BlockPacker,
    private val stripeWriter: StripeWriter,
    private val recordWriter: RecordWriter,
    private val manifest: AkkManifest
) : AutoCloseable {

    private val lock = ReentrantReadWriteLock()

    /* -------- API -------- */

    fun put(rec: Record) = lock.write {
        memTable.put(rec)
    }

    fun get(key: ByteBuffer): ByteBuffer? = lock.read {
        memTable.get(key)?.value
        // TODO: SSTable & StripeReader fallback
    }

    /** Flushes MemTable → disk and fsyncs manifest. */
    fun flush() = lock.write {
        memTable.flush()
        val stripes = stripeWriter.flush()
        manifest.advance(stripes)
    }

    override fun close() {
        flush()
        stripeWriter.close()
        (packer as? AutoCloseable)?.close()
    }

    /* -------- Factory -------- */

    companion object {
        /** Build a fully‑wired DB instance with default components. */
        fun open(
            stripeWriter: StripeWriter,
            recordWriter: RecordWriter,
            packer: BlockPacker,
            manifest: AkkManifest,
            flushThresholdBytes: Long = 64L * 1024 * 1024
        ): AkkaraDB {
            lateinit var db: AkkaraDB   // forward ref for lambda

            val mem = MemTable(flushThresholdBytes) { records ->
                records.forEach { rec ->
                    val maxSize = recordWriter.computeMaxSize(rec)
                    val tmp = ByteBuffer.allocate(maxSize)
                    recordWriter.write(rec, tmp)
                    tmp.flip()
                    packer.addRecord(tmp) { blk -> stripeWriter.addBlock(blk) }
                }
                packer.flush { blk -> stripeWriter.addBlock(blk) }
            }

            db = AkkaraDB(mem, packer, stripeWriter, recordWriter, manifest).also {
                manifest.load()               // recover counter on open
            }
            return db
        }
    }
}
