package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.engine.wal.replayWal
import dev.swiftstorm.akkaradb.format.akk.*
import dev.swiftstorm.akkaradb.format.akk.parity.XorParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Core database engine wired together with MemTable, WAL, BlockPacker, and StripeWriter.
 */
class AkkaraDB private constructor(
    private val memTable: MemTable,
    private val packer: AkkBlockPackerDirect,
    private val stripeWriter: AkkStripeWriter,
    private val recordWriter: AkkRecordWriter,
    private val wal: WalWriter,
    private val manifest: AkkManifest
) : AutoCloseable {

    private val lock = ReentrantReadWriteLock()

    /* -------------------------------------------------- */
    /*                    public API                      */
    /* -------------------------------------------------- */

    fun put(rec: Record) = lock.write {
        // 1) encode record ➜ tmp ByteBuffer
        val bufSize = recordWriter.computeMaxSize(rec)
        val tmp = ByteBuffer.allocate(bufSize)
        recordWriter.write(rec, tmp)
        tmp.flip()

        // 2) write‑ahead log first (durable)
        wal.append(tmp.duplicate())

        // 3) update MemTable & BlockPacker
        memTable.put(rec)
        packer.addRecord(tmp)
    }

    fun get(key: ByteBuffer): ByteBuffer? = lock.read {
        memTable.get(key)?.value
        // TODO: SSTable & StripeReader fallback
    }

    /** Flush MemTable ➜ BlockPacker ➜ StripeWriter and fsync manifest + WAL checkpoint. */
    fun flush() = lock.write {
        memTable.flush()
        val stripes = stripeWriter.flush()
        manifest.advance(stripes)
        wal.checkpoint(stripes, memTable.lastSeq())
        wal.sealSegment()
    }

    override fun close() {
        flush()
        stripeWriter.close()
        wal.close()
        packer.close()
    }

    /* -------------------------------------------------- */
    /*                 factory / opener                   */
    /* -------------------------------------------------- */

    companion object {
        fun open(
            baseDir: Path,
            k: Int = 4,
            parityCoder: ParityCoder = XorParityCoder(),
            flushThresholdBytes: Long = 64L * 1024 * 1024
        ): AkkaraDB {
            // 1. persistent components
            val manifest = AkkManifest(baseDir.resolve("manifest.txt"))
            val stripe = AkkStripeWriter(baseDir, k, parityCoder, autoFlush = true)
            val wal = WalWriter(baseDir.resolve("wal.log"))
            val writer = AkkRecordWriter

            // 2. in‑memory & pipeline components
            val packer = AkkBlockPackerDirect({ blk -> stripe.addBlock(blk) })

            val mem = MemTable(flushThresholdBytes) { records ->
                records.forEach { rec ->
                    val size = writer.computeMaxSize(rec)
                    val buf = ByteBuffer.allocate(size)
                    writer.write(rec, buf)
                    buf.flip()
                    packer.addRecord(buf)
                }
                packer.flush()
            }

            // 3. WAL replay before opening for writes
            replayWal(baseDir.resolve("wal.log"), mem)

            // 4. create engine instance
            return AkkaraDB(mem, packer, stripe, writer, wal, manifest).also { db ->
                // load manifest & seek StripeWriter
                manifest.load()
                stripe.seek(manifest.stripesWritten)

                // boot‑time recovery: read existing stripes back into MemTable
                val reader = AkkStripeReader(baseDir = stripe.baseDir, k = stripe.k, parityCoder = stripe.parityCoder)
                var stripeId = 0L
                val recReader = AkkRecordReader
                while (true) {
                    val payloads = reader.readStripe() ?: break
                    for (payload in payloads) {
                        val dup = payload.duplicate()
                        while (dup.hasRemaining()) {
                            val rec = recReader.read(dup)
                            mem.put(rec)
                        }
                    }
                    stripeId++
                }
                reader.close()
            }
        }
    }
}
