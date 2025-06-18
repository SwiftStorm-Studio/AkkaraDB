package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.format.akk.*
import dev.swiftstorm.akkaradb.format.akk.parity.XorParityCoder
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import dev.swiftstorm.akkaradb.format.api.RecordWriter
import dev.swiftstorm.akkaradb.format.api.StripeWriter
import java.nio.ByteBuffer
import java.nio.file.Path
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
        fun open(
            baseDir: Path,
            k: Int = 4,
            parityCoder: ParityCoder = XorParityCoder(),
            flushThresholdBytes: Long = 64L * 1024 * 1024
        ): AkkaraDB {
            val manifest = AkkManifest(baseDir.resolve("manifest.txt"))
            val stripe = AkkStripeWriter(baseDir, k, parityCoder, true)
            val packer = AkkBlockPackerDirect
            val writer = AkkRecordWriter

            return with(
                stripeWriter = stripe,
                recordWriter = writer,
                packer = packer,
                manifest = manifest,
                flushThresholdBytes = flushThresholdBytes
            )
        }

        fun with(
            stripeWriter: StripeWriter,
            recordWriter: RecordWriter,
            packer: BlockPacker,
            manifest: AkkManifest,
            flushThresholdBytes: Long = 64L * 1024 * 1024
        ): AkkaraDB {

            val mem = MemTable(flushThresholdBytes) { records ->
                records.forEach { rec ->
                    val bufSize = recordWriter.computeMaxSize(rec)
                    val tmp = ByteBuffer.allocate(bufSize)
                    recordWriter.write(rec, tmp)
                    tmp.flip()
                    packer.addRecord(tmp) { blk -> stripeWriter.addBlock(blk) }
                }
                packer.flush { blk -> stripeWriter.addBlock(blk) }
            }

            return AkkaraDB(mem, packer, stripeWriter, recordWriter, manifest).also {
                manifest.load()
                stripeWriter.seek(manifest.stripesWritten)

                val reader = AkkStripeReader(
                    baseDir = (stripeWriter as AkkStripeWriter).baseDir,
                    k = stripeWriter.k,
                    parityCoder = stripeWriter.parityCoder
                )
                val recReader = AkkRecordReader
                var stripeId = 0L
                loop@ while (true) {
                    val payloads = reader.readStripe() ?: break@loop
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
