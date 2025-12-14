/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.*
import dev.swiftstorm.akkaradb.common.types.U64
import dev.swiftstorm.akkaradb.engine.logging.AkkLogger
import dev.swiftstorm.akkaradb.engine.logging.impl.AkkLoggerImpl
import dev.swiftstorm.akkaradb.engine.manifest.AkkManifest
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.engine.sstable.RefCountedSSTableReader
import dev.swiftstorm.akkaradb.engine.sstable.SSTCompactor
import dev.swiftstorm.akkaradb.engine.sstable.SSTableReader
import dev.swiftstorm.akkaradb.engine.sstable.SSTableWriter
import dev.swiftstorm.akkaradb.engine.util.PeekingIterator
import dev.swiftstorm.akkaradb.engine.wal.WalOp
import dev.swiftstorm.akkaradb.engine.wal.WalReplay
import dev.swiftstorm.akkaradb.engine.wal.WalWriter
import dev.swiftstorm.akkaradb.format.akk.*
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
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * AkkaraDB is a high-level database class designed to provide a persisted key-value store
 * with features such as write-ahead logging (WAL), in-memory storage with MemTable, and SSTable-based storage layers.
 * It is meant to combine fast in-memory operations with reliable persistence mechanisms.
 *
 * The class includes a factory for initialization via provided configuration options through the `Options` data class.
 * AkkaraDB supports standard database operations and ensures consistency and durability.
 *
 * This private constructor prevents direct instantiation, and instances are expected to be created using the `open` method.
 */
class AkkaraDB private constructor(
    private val baseDir: Path,
    private val wal: WalWriter,
    private val mem: MemTable,
    private val manifest: AkkManifest,
    private val stripeW: AkkStripeWriter,
    private val stripeR: AkkStripeReader?,
    private val readers: ConcurrentLinkedDeque<RefCountedSSTableReader>,
    private val durableCas: Boolean,
    private val useStripeForRead: Boolean,
    private val logger: AkkLogger
) : Closeable {
    private val unpacker = AkkBlockUnpacker()

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
        mem.get(key)?.let { rec ->
            return if (rec.tombstone) null else rec.value
        }

        // 2) SST newest-first (snapshot + acquire)
        val snapshot = readers.toList()
        for (r in snapshot) {
            if (!r.acquire()) continue
            try {
                val v = r.get(key)
                if (v != null) return v
            } finally {
                r.release()
            }
        }

        // 3) Stripe fallback
        if (useStripeForRead) stripeFallbackLookup(key)?.let { return it }
        return null
    }

    /** CAS by expected sequence: if matches current MemTable, set new value (or delete if null). */
    fun compareAndSwap(key: ByteBufferL, expectedSeq: Long, newValue: ByteBufferL?): Boolean {
        val ok = mem.compareAndSwap(key.duplicate(), expectedSeq, newValue?.duplicate())
        val nextSeq = mem.lastSeq() // Advancing by MemTable
        if (ok && durableCas) {
            // Log as an idempotent WAL op with the same seq so recovery converges.
            val keyBytes = key.duplicate().position(0).readAllBytes()
            if (newValue == null) {
                val op = WalOp.Delete(
                    key = keyBytes,
                    seq = U64.fromSigned(nextSeq),
                    tombstoneFlag = RecordFlags.TOMBSTONE,
                    keyFP64 = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED),
                    miniKey = AKHdr32.buildMiniKeyLE(key)
                )
                wal.append(op)
            } else {
                val valBytes = newValue.duplicate().position(0).readAllBytes()
                val op = WalOp.Add(
                    key = keyBytes,
                    value = valBytes,
                    seq = U64.fromSigned(nextSeq),
                    flags = 0,
                    keyFP64 = AKHdr32.sipHash24(key, AKHdr32.DEFAULT_SIPHASH_SEED),
                    miniKey = AKHdr32.buildMiniKeyLE(key)
                )
                wal.append(op)
            }
        }
        return ok
    }

    fun range(start: ByteBufferL, end: ByteBufferL): Sequence<MemRecord> = sequence {
        val snapshot = readers.toList()
        val acquiredReaders = snapshot.filter { it.acquire() }

        try {
            val iters = ArrayList<PeekingIterator<MemRecord>>(acquiredReaders.size + 1)

            // MemTable iterator
            iters.add(PeekingIterator(mem.iterator(MemTable.KeyRange(start, end)).iterator()))

            // SST iterators
            for (r in acquiredReaders) {
                val sstIter = r.range(start, end).map { (k, v, hdr) ->
                    val tombstone = hdr.flags and RecordFlags.TOMBSTONE.toInt() != 0
                    val actualValue = if (tombstone) MemRecord.EMPTY else v.asReadOnlyDuplicate()
                    MemRecord(
                        key = k.asReadOnlyDuplicate(),
                        value = actualValue,
                        seq = hdr.seq.raw,
                        flags = hdr.flags.toByte(),
                        keyHash = hdr.keyFP64.raw.toInt(),
                        approxSizeBytes = estimateMemFootprint(k, actualValue)
                    )
                }.iterator()
                iters.add(PeekingIterator(sstIter))
            }

            val batch = ArrayList<MemRecord>(1024)

            // K-way merge
            while (true) {
                iters.removeIf { !it.hasNext() }
                if (iters.isEmpty()) break

                val first = iters.minWithOrNull { a, b -> lexCompare(a.peek().key, b.peek().key) } ?: break
                val rec = first.next()

                // Dedup
                while (true) {
                    val next = iters.filter { it.hasNext() }
                        .minByOrNull { lexCompare(it.peek().key, rec.key) }
                    if (next == null || !next.hasNext()) break
                    val peek = next.peek()
                    if (lexCompare(peek.key, rec.key) != 0) break
                    next.next()
                }

                if (!rec.tombstone) {
                    batch.add(rec)
                    if (batch.size >= 1024) {
                        yieldAll(batch)
                        batch.clear()
                    }
                }
            }

            if (batch.isNotEmpty()) yieldAll(batch)

        } finally {
            acquiredReaders.forEach { it.release() }
        }
    }

    /** Best-effort flush: hint MemTable, seal any complete stripes, and checkpoint manifest. */
    fun flush() {
        logger.debug("Phase: AkkaraDB.flush called")
        mem.flushHint()
        logger.debug("Phase: MemTable.flushHint completed")
        val sealed = stripeW.sealIfComplete()
        logger.debug("Phase: StripeWriter.sealIfComplete completed; sealed=$sealed")
        if (sealed) stripeW.flush(FlushMode.SYNC)
        logger.debug("Phase: StripeWriter.flush completed")
        manifest.checkpoint(name = "flush", stripe = stripeW.lastSealedStripe, lastSeq = mem.lastSeq())
        logger.debug("Phase: Manifest.checkpoint completed")
        wal.forceSync()
        logger.debug("Phase: Wal.forceSync completed")
    }

    fun lastSeq(): Long = mem.lastSeq()

    override fun close() {
        try {
            logger.debug("Phase: flushing and closing AkkaraDB...")
            flush()
        } finally {
            runCatching { logger.debug("Phase: mem.close"); mem.close() }
            runCatching { logger.debug("Phase: stripeW.close"); stripeW.close() }
            runCatching { logger.debug("Phase: readers.close"); readers.forEach { it.close() } }
            runCatching { logger.debug("Phase: manifest.close"); manifest.close() }
            runCatching { logger.debug("Phase: wal.close"); wal.close() }
        }
    }

    // ---------------- factory ----------------

    data class Options(
        val baseDir: Path,
        val k: Int = 4,
        val m: Int = 2,
        val flushPolicy: FlushPolicy = FlushPolicy(maxBlocks = 32, maxMicros = 500),
        val walFastMode: Boolean = true,
        val stripeFastMode: Boolean = true,
        val walGroupN: Int = 64,
        val walGroupMicros: Long = 1_000,
        val parityCoder: ParityCoder? = null,
        val durableCas: Boolean = false,
        val useStripeForRead: Boolean = false,
        val bloomFPRate: Double = 0.01,
        val debug: Boolean = false
    )

    companion object {
        lateinit var logger: AkkLogger

        fun open(opts: Options): AkkaraDB {
            logger = AkkLoggerImpl { opts.debug }

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
                fastMode = opts.stripeFastMode
            )

            // Prepare compactor and reader management
            val compactor = SSTCompactor(sstDir, manifest = manifest)
            fun rebuildReaders(into: ConcurrentLinkedDeque<RefCountedSSTableReader>) {
                val oldReaders = ArrayList(into)
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

                // Sort by level (L0, L1, ...) then by mtime desc within level
                toOpen.sortWith { a, b ->
                    fun parseLevel(p: Path): Int {
                        val parent = p.parent?.fileName?.toString() ?: return Int.MAX_VALUE
                        return if (parent.startsWith("L")) parent.substring(1).toIntOrNull() ?: Int.MAX_VALUE else Int.MAX_VALUE
                    }

                    val la = parseLevel(a)
                    val lb = parseLevel(b)
                    if (la != lb) la - lb else Files.getLastModifiedTime(b).compareTo(Files.getLastModifiedTime(a))
                }

                toOpen.forEach { p ->
                    val ch = FileChannel.open(p, READ)
                    val inner = SSTableReader.open(ch)
                    into.addLast(RefCountedSSTableReader(inner))
                }

                oldReaders.forEach { it.close() }
            }

            // Prepare onFlush callback: write L0 SST and optionally pack into stripes
            val readersDeque = ConcurrentLinkedDeque<RefCountedSSTableReader>()
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
                        CREATE, WRITE, READ, TRUNCATE_EXISTING
                    ).use { ch ->
                        SSTableWriter(ch, expectedEntries = sorted.size.toLong(), opts.bloomFPRate).use { w ->
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
                        val sealed = stripe.sealIfComplete()
                        if (sealed) {
                            if (!stripe.isFastMode) stripe.flush(FlushMode.SYNC) else stripe.flush(FlushMode.ASYNC)
                        }
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
            val wal = WalWriter(walPath, groupN = opts.walGroupN, groupTmicros = opts.walGroupMicros, fastMode = opts.walFastMode)

            logger.debug("WAL path: $walPath")
            logger.debug("WAL exists: ${Files.exists(walPath)}")
            if (Files.exists(walPath)) {
                logger.debug("WAL size: ${Files.size(walPath)} bytes")
            }

            // Recovery: WAL → MemTable
            val replayResult = WalReplay.replay(walPath, mem)
            logger.debug("WAL replay result: ${replayResult.applied} entries")

            runCatching {
                val rec = stripe.recover()
                if (rec.truncatedTail) {
                    // Truncated partial blocks: enforce exact truncation
                    val last = rec.lastSealed
                    val exactSize = if (last >= 0) (last + 1) * stripe.blockSize.toLong() else 0L
                    // truncate all lanes to same size
                    for (ch in stripe.dataChannels()) ch.truncate(exactSize)
                    for (ch in stripe.parityChannels()) ch.truncate(exactSize)
                }

                // Optionally, update manifest state
                manifest.checkpoint(
                    name = "stripeRecover",
                    stripe = rec.lastDurable,
                    lastSeq = mem.lastSeq()
                )
            }.onFailure { error("AkkStripeWriter recover failed: $it") }

            // Load existing SSTs from all levels (newest-first)
            rebuildReaders(readersDeque)

            val stripeReader = if (opts.useStripeForRead) AkkStripeReader(opts.k, opts.m, laneDir, pool, stripe.coder()) else null

            return AkkaraDB(
                base,
                wal,
                mem,
                manifest,
                stripe,
                stripeReader,
                readersDeque,
                opts.durableCas,
                opts.useStripeForRead,
                logger
            )
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

    // ---------------- private ----------------

    private fun stripeFallbackLookup(key: ByteBufferL): ByteBufferL? {
        val reader = stripeR ?: return null
        val kdup = key.duplicate()
        reader.seek(0)

        var bestSeq = Long.MIN_VALUE
        var bestValue: ByteBufferL? = null
        var bestIsTombstone = false

        // Debug用
        var stripesScanned = 0
        var keysFound = 0

        while (true) {
            val stripe = reader.readStripe() ?: break
            stripesScanned++

            stripe.use {
                for (block in it.payloads) {
                    val cursor = unpacker.cursor(block)
                    while (cursor.hasNext()) {
                        val rec = cursor.next()
                        if (lexCompare(rec.key, kdup) == 0) {
                            keysFound++
                            val seq = rec.seq.raw

                            if (seq > bestSeq) {
                                bestSeq = seq
                                bestIsTombstone = (rec.flags and 1) != 0
                                bestValue = if (bestIsTombstone) null else rec.value.copy()
                            } else if (seq == bestSeq) {
                                val isTombstone = (rec.flags and 1) != 0
                                if (isTombstone && !bestIsTombstone) {
                                    bestIsTombstone = true
                                    bestValue = null
                                }
                            }
                        }
                    }
                }
            }
        }

        if (keysFound > 0) {
            logger.debug(
                "[StripeFallback] key=${firstKeyHex(kdup)}, " +
                        "stripes=$stripesScanned, versions=$keysFound, " +
                        "bestSeq=$bestSeq, tombstone=$bestIsTombstone"
            )
        }

        return bestValue
    }
}