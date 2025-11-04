package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.sstable.SSTCompactor
import dev.swiftstorm.akkaradb.engine.sstable.SSTableReader
import dev.swiftstorm.akkaradb.engine.sstable.SSTableWriter
import dev.swiftstorm.akkaradb.engine.wal.WalOp
import dev.swiftstorm.akkaradb.engine.wal.WalReplay
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.format.akk.AkkBlockPacker
import dev.swiftstorm.akkaradb.format.akk.AkkStripeWriter
import dev.swiftstorm.akkaradb.format.akk.parity.DualXorParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.NoParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.XorParityCoder
import dev.swiftstorm.akkaradb.format.api.FlushMode
import dev.swiftstorm.akkaradb.format.api.FlushPolicy
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.io.Closeable
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * AkkaraDB v3 engine implementation (minimal, spec-aligned).
 *
 * Responsibilities:
 * - Durable writes via WAL (group-commit N or T) before acknowledging.
 * - In-memory MemTable with shard-level concurrency and flush callback.
 * - SSTable emission on flush (L0), newest-first reader list for lookups.
 * - Optional stripe packing of data blocks (32 KiB) for fast sequential scans.
 * - Simple manifest logging for SST seals and checkpoints.
 */
class AkkaraDB private constructor(
    private val baseDir: Path,
    private val wal: WalWriter,
    private val mem: MemTable,
    private val manifest: AkkManifest,
    private val stripe: AkkStripeWriter,
    private val readers: ConcurrentLinkedDeque<SSTableReader>,
) : Closeable {


    // ---------------- API ----------------

    /** Put key/value; returns assigned global sequence number. */
    fun put(key: ByteBufferL, value: ByteBufferL): Long {
        val seq = mem.nextSeq()
        // Compute header-derived miniKey/fp for WAL payload
        val keyBytes = key.duplicate().position(0).readAllBytes()
        val valBytes = value.duplicate().position(0).readAllBytes()
        val op = WalOp.Add(
            key = keyBytes,
            value = valBytes,
            seq = U64.fromSigned(seq),
            flags = 0,
            keyFP64 = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED),
            miniKey = AKHdr32.buildMiniKeyLE(key)
        )
        wal.append(op) // durable before apply
        mem.put(key.duplicate(), value.duplicate(), seq)
        return seq
    }

    /** Insert a tombstone for key; returns assigned sequence. */
    fun delete(key: ByteBufferL): Long {
        val seq = mem.nextSeq()
        val keyBytes = key.duplicate().position(0).readAllBytes()
        val op = WalOp.Delete(
            key = keyBytes,
            seq = U64.fromSigned(seq),
            tombstoneFlag = 0x01,
            keyFP64 = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED),
            miniKey = AKHdr32.buildMiniKeyLE(key)
        )
        wal.append(op)
        mem.delete(key.duplicate(), seq)
        return seq
    }

    /** Get value for key, or null if missing or tombstoned. */
    fun get(key: ByteBufferL): ByteBufferL? {
        // 1) MemTable fast path
        mem.get(key)?.let { rec -> return if (rec.tombstone) null else rec.value }
        // 2) SST newest-first
        for (r in readers) {
            val v = r.get(key)
            if (v != null) return v
        }
        return null
    }

    /** CAS by expected sequence: if matches current MemTable, set new value (or delete if null). */
    fun compareAndSwap(key: ByteBufferL, expectedSeq: Long, newValue: ByteBufferL?): Boolean {
        // Minimal engine: CAS is in-memory only (non-durable). Returns true on success.
        return mem.compareAndSwap(key.duplicate(), expectedSeq, newValue?.duplicate())
    }

    /** Iterate in-memory snapshot across shards. For full DB iteration, merge SSTs as needed (not provided). */
    fun iterator(range: MemTable.KeyRange = MemTable.KeyRange.ALL): Sequence<MemRecord> = mem.iterator(range)

    /** Best-effort flush: hint MemTable, seal any complete stripes, and checkpoint manifest. */
    fun flush() {
        mem.flushHint()
        stripe.flush(FlushMode.SYNC)
        manifest.checkpoint(name = "flush", stripe = stripe.lastSealedStripe, lastSeq = mem.lastSeq())
    }

    fun lastSeq(): Long = mem.lastSeq()

    override fun close() {
        try {
            flush()
        } finally {
            runCatching { mem.close() }
            runCatching { stripe.close() }
            runCatching { readers.forEach { it.close() } }
            runCatching { manifest.close() }
            runCatching { wal.close() }
        }
    }

    // ---------------- factory ----------------

    data class Options(
        val baseDir: Path,
        val k: Int = 4,
        val m: Int = 2,
        val flushPolicy: FlushPolicy = FlushPolicy(maxBlocks = 32, maxMicros = 500),
        val fastMode: Boolean = true,
        val walGroupN: Int = 32,
        val walGroupMicros: Long = 500,
        val parityCoder: ParityCoder? = null
    )

    companion object {
        fun open(opts: Options): AkkaraDB {
            val base = opts.baseDir
            Files.createDirectories(base)
            val sstDir = base.resolve("sst")
            val laneDir = base.resolve("lanes")
            Files.createDirectories(sstDir)
            Files.createDirectories(laneDir)
            // Ensure L0 directory for compaction pipeline exists
            val l0Dir = sstDir.resolve("L0")
            Files.createDirectories(l0Dir)

            val manifest = AkkManifest(base.resolve("manifest.akmf"), fastMode = true)
            manifest.load()
            manifest.start()

            val pool = Pools.io()

            val stripe = AkkStripeWriter(
                k = opts.k,
                m = opts.m,
                laneDir = laneDir,
                pool = pool,
                coder = opts.parityCoder ?: when (opts.m) {
                    0 -> NoParityCoder()
                    1 -> XorParityCoder()
                    2 -> DualXorParityCoder()
                    else -> RSParityCoder(opts.m)
                },
                flushPolicy = opts.flushPolicy,
                fastMode = opts.fastMode
            )

            // Prepare compactor and reader management
            val compactor = SSTCompactor(sstDir)
            fun rebuildReaders(into: ConcurrentLinkedDeque<SSTableReader>) {
                // Close existing
                into.forEach { runCatching { it.close() } }
                into.clear()
                val toOpen = ArrayList<Path>()
                if (Files.isDirectory(sstDir)) {
                    Files.list(sstDir).use { lvlStream ->
                        lvlStream.filter { Files.isDirectory(it) && it.fileName.toString().startsWith("L") }
                            .forEach { lvlDir ->
                                Files.list(lvlDir).use { fs ->
                                    fs.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".sst") }
                                        .forEach { toOpen.add(it) }
                                }
                            }
                    }
                    // backward-compat: flat .aksst files
                    Files.list(sstDir).use { rootStream ->
                        rootStream.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".aksst") }
                            .forEach { toOpen.add(it) }
                    }
                }
                toOpen.sortWith { a, b -> Files.getLastModifiedTime(b).compareTo(Files.getLastModifiedTime(a)) }
                toOpen.forEach { p ->
                    val ch = FileChannel.open(p, StandardOpenOption.READ)
                    val r = SSTableReader.open(ch)
                    into.addLast(r)
                }
            }

            // Prepare onFlush callback: write L0 SST and optionally pack into stripes
            val readersDeque = ConcurrentLinkedDeque<SSTableReader>()
            val onFlushCb: (List<MemRecord>) -> Unit = { batch ->
                if (batch.isEmpty()) {
                    manifest.checkpoint(name = "memFlush-empty")
                } else {
                    // Sort by key lexicographically
                    val sorted = ArrayList(batch)
                    java.util.Collections.sort(sorted, java.util.Comparator { a, b -> lexCompare(a.key, b.key) })

                    // Write SST (L0)
                    val file = l0Dir.resolve("L0_" + System.nanoTime().toString() + ".sst")
                    FileChannel.open(
                        file,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
                    ).use { ch ->
                        SSTableWriter(ch, expectedEntries = sorted.size.toLong()).use { w ->
                            w.writeAll(sorted.asSequence())
                            w.seal()
                        }
                    }

                    // Pack to stripes (optional; only if at least k records to form blocks)
                    runCatching {
                        val packer = AkkBlockPacker(onBlockReady = { full -> stripe.addBlock(full) }, pool = pool)
                        packer.beginBlock()
                        for (r in sorted) {
                            if (!packer.tryAppend(
                                    r.key.duplicate(),
                                    if (r.tombstone) ByteBufferL.allocate(0, false) else r.value.duplicate(),
                                    U64.fromSigned(r.seq),
                                    (r.flags.toInt() and 0xFF),
                                    AKHdr32.sipHash24(r.key, AKHdr32.DEFAULT_SIPHASH_SEED),
                                    AKHdr32.buildMiniKeyLE(r.key)
                                )
                            ) {
                                packer.endBlock(); packer.beginBlock()
                                check(
                                    packer.tryAppend(
                                        r.key.duplicate(),
                                        if (r.tombstone) ByteBufferL.allocate(0, false) else r.value.duplicate(),
                                        U64.fromSigned(r.seq),
                                        (r.flags.toInt() and 0xFF),
                                        AKHdr32.sipHash24(r.key, AKHdr32.DEFAULT_SIPHASH_SEED),
                                        AKHdr32.buildMiniKeyLE(r.key)
                                    )
                                )
                            }
                        }
                        packer.flush()
                        packer.close()
                        stripe.sealIfComplete()
                        if (!stripe.isFastMode) stripe.flush(FlushMode.SYNC) else stripe.flush(FlushMode.ASYNC)
                    }.onFailure { /* stripes are optional; ignore in minimal engine */ }

                    val firstKey = sorted.first().key
                    val lastKey = sorted.last().key
                    manifest.sstSeal(
                        level = 0,
                        file = "L0/" + file.fileName.toString(),
                        entries = sorted.size.toLong(),
                        firstKeyHex = firstKeyHex(firstKey),
                        lastKeyHex = firstKeyHex(lastKey)
                    )

                    // Run compaction and rebuild readers to reflect new levels
                    runCatching { compactor.compact() }
                    rebuildReaders(readersDeque)

                    manifest.checkpoint(name = "memFlush", stripe = stripe.lastSealedStripe, lastSeq = sorted.last().seq)
                }
            }

            val mem = MemTable(onFlush = onFlushCb)

            // WAL
            val walPath = base.resolve("wal.akwal")
            val wal = WalWriter(walPath, groupN = opts.walGroupN, groupTmicros = opts.walGroupMicros)

            // Recovery: WAL → MemTable
            WalReplay.replay(walPath, mem)

            // Load existing SSTs from all levels (newest-first)
            rebuildReaders(readersDeque)

            return AkkaraDB(base, wal, mem, manifest, stripe, readersDeque)
        }

        private fun ByteBufferL.readAllBytes(): ByteArray {
            val d = this.duplicate().position(0)
            val arr = ByteArray(d.remaining)
            var i = 0
            while (d.remaining > 0) {
                arr[i++] = d.i8.toByte()
            }
            return arr
        }

        private fun firstKeyHex(k: ByteBufferL): String = buildString {
            val d = k.duplicate().position(0)
            var i = 0
            while (d.remaining > 0 && i < 16) { // first up to 16 bytes as hex
                append(String.format("%02x", d.i8))
                i++
            }
        }
    }
}