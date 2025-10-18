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

package dev.swiftstorm.akkaradb.engine.manifest

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32C
import kotlin.concurrent.thread

/**
 * AkkManifest (v3+)
 *
 * Append-only manifest with length-prefixed JSON records and CRC32C.
 * - Durable mode: append one record and force(true) every time.
 * - Fast mode   : batch N or T µs, force(false) each batch, periodic force(true).
 *
 * New in v3+:
 *  - Events: CompactionStart / CompactionEnd / SSTDelete / Truncate / FormatBump.
 *  - Rotation: roll to a new file when size exceeds [rotateMaxBytes].
 *  - Snapshot: in-memory view of live/deleted SSTs for compactor/planner.
 */
class AkkManifest(
    private val path: Path,
    val fastMode: Boolean = false,
    // Fast mode tuning (N/T/S)
    private val batchMaxEvents: Int = 128,
    private val batchMaxMicros: Long = 500,
    private val strongSyncIntervalMillis: Long = 1000,
    // Rotation
    private val rotateMaxBytes: Long = 32L * 1024 * 1024 // 32 MiB default
) : Closeable {

    /* ─────────── Replayed state (visible to engine) ─────────── */

    @Volatile
    var stripesWritten: Long = 0L
        private set

    @Volatile
    var lastSstSeal: SstSealEvent? = null
        private set

    @Volatile
    var lastCheckpoint: CheckpointEvent? = null
        private set

    /** Live SST file names visible to readers/compactor. */
    private val liveSst = linkedSetOf<String>()

    /** Deleted SST file names (tombstones for ops). */
    private val deletedSst = linkedSetOf<String>()

    val sstSeals = mutableListOf<SstSealEvent>()

    data class SstSealEvent(
        val level: Int,
        val file: String,
        val entries: Long,
        val firstKeyHex: String?,
        val lastKeyHex: String?,
        val ts: Long
    )

    data class CheckpointEvent(
        val name: String?,
        val stripe: Long?,
        val lastSeq: Long?,
        val ts: Long
    )

//    data class CompactionStartEvent(
//        val level: Int,
//        val inputs: List<String>,
//        val ts: Long
//    )
//
//    data class CompactionEndEvent(
//        val level: Int,
//        val output: String,
//        val inputs: List<String>,
//        val entries: Long,
//        val firstKeyHex: String?,
//        val lastKeyHex: String?,
//        val ts: Long
//    )

    data class Snapshot(
        val stripesWritten: Long,
        val liveSst: List<String>,
        val deletedSst: List<String>,
        val lastSstSeal: SstSealEvent?,
        val lastCheckpoint: CheckpointEvent?
    )

    /* ─────────── Fast-mode internals ─────────── */

    private val running = AtomicBoolean(false)
    private val q = ConcurrentLinkedQueue<String>()
    @Volatile
    private var flusherThread: Thread? = null
    @Volatile
    private var fastCh: FileChannel? = null
    @Volatile
    private var lastStrongSyncNanos: Long = 0L

    /* ─────────── Public API ─────────── */

    /** Load and replay the manifest file into memory (idempotent). */
    fun load() {
        if (!Files.exists(path)) return
        FileChannel.open(path, READ).use { ch ->
            val lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
            var total = 0L
            while (true) {
                lenBuf.clear()
                if (!readFully(ch, lenBuf)) break
                lenBuf.flip()
                val len = lenBuf.int
                if (len <= 0) break

                val recBuf = ByteBuffer.allocate(len + 4).order(ByteOrder.LITTLE_ENDIAN)
                if (!readFully(ch, recBuf)) break
                recBuf.flip()

                val data = ByteArray(len)
                recBuf.get(data)
                val storedCrc = recBuf.int
                val crc = CRC32C().apply { update(data) }.value.toInt()
                if (crc != storedCrc) break

                var line = String(data, UTF_8)
                if (line.isNotEmpty() && line[0] == '\uFEFF') line = line.substring(1)
                line = line.trim()
                if (line.isNotEmpty()) applyEventLine(line)

                total += 4 + len + 4
            }
        }
    }

    /** Start fast-mode background flusher (no-op for durable mode). */
    @Synchronized
    fun start() {
        if (!fastMode || running.get()) return
        Files.createDirectories(path.parent)
        fastCh = FileChannel.open(path, CREATE, WRITE, APPEND)
        running.set(true)
        lastStrongSyncNanos = System.nanoTime()
        flusherThread = thread(start = true, isDaemon = true, name = "akk-manifest-flusher") {
            runFlusher()
        }
    }

    /** Append StripeCommit(after=newCount). */
    @Synchronized
    fun advance(newCount: Long) {
        require(newCount >= stripesWritten) { "stripe counter must be monotonic" }
        stripesWritten = newCount
        append("""{"type":"StripeCommit","after":$newCount,"ts":${now()}}""")
        maybeRotate()
    }

    /** Append SSTSeal and mark file as live. */
    @Synchronized
    fun sstSeal(level: Int, file: String, entries: Long, firstKeyHex: String?, lastKeyHex: String?) {
        val ts = now()
        val json = buildString {
            append("{\"type\":\"SSTSeal\",")
            append("\"level\":").append(level).append(',')
            append("\"file\":\"").append(esc(file)).append("\",")
            append("\"entries\":").append(entries).append(',')
            if (firstKeyHex != null) append("\"firstKeyHex\":\"").append(esc(firstKeyHex)).append("\",")
            if (lastKeyHex != null) append("\"lastKeyHex\":\"").append(esc(lastKeyHex)).append("\",")
            append("\"ts\":").append(ts).append("}")
        }
        append(json)

        val e = SstSealEvent(level, file, entries, firstKeyHex, lastKeyHex, ts)
        sstSeals += e
        lastSstSeal = e
        liveSst += file
        deletedSst.remove(file)

        maybeRotate()
    }

    /** Append Checkpoint(name,stripe,lastSeq). */
    @Synchronized
    fun checkpoint(name: String? = null, stripe: Long? = null, lastSeq: Long? = null) {
        val ts = now()
        val json = buildString {
            append("{\"type\":\"Checkpoint\",")
            if (name != null) append("\"name\":\"").append(esc(name)).append("\",")
            if (stripe != null) append("\"stripe\":").append(stripe).append(',')
            if (lastSeq != null) append("\"lastSeq\":").append(lastSeq).append(',')
            append("\"ts\":").append(ts).append("}")
        }
        append(json)
        lastCheckpoint = CheckpointEvent(name, stripe, lastSeq, ts)
        maybeRotate()
    }

    /** Mark compaction start (level, input files). */
    @Synchronized
    fun compactionStart(level: Int, inputs: List<String>) {
        val ts = now()
        val arr = inputs.joinToString(separator = "\",\"", prefix = "[\"", postfix = "\"]") { esc(it) }
        append("""{"type":"CompactionStart","level":$level,"inputs":$arr,"ts":$ts}""")
        maybeRotate()
    }

    /** Mark compaction end (level, output, inputs, summary). */
    @Synchronized
    fun compactionEnd(
        level: Int,
        output: String,
        inputs: List<String>,
        entries: Long,
        firstKeyHex: String?,
        lastKeyHex: String?
    ) {
        val ts = now()
        val arr = inputs.joinToString(separator = "\",\"", prefix = "[\"", postfix = "\"]") { esc(it) }
        val fk = firstKeyHex?.let { "\"firstKeyHex\":\"${esc(it)}\"," } ?: ""
        val lk = lastKeyHex?.let { "\"lastKeyHex\":\"${esc(it)}\"," } ?: ""
        append("""{"type":"CompactionEnd","level":$level,"output":"${esc(output)}","inputs":$arr,"entries":$entries,$fk$lk"ts":$ts}""")

        // State: add output live, mark inputs deleted (but we keep the names)
        liveSst += output
        inputs.forEach { f -> liveSst.remove(f); deletedSst += f }

        maybeRotate()
    }

    /** Explicitly mark an SST as deleted (e.g., failed compaction cleanup). */
    @Synchronized
    fun sstDelete(file: String) {
        append("""{"type":"SSTDelete","file":"${esc(file)}","ts":${now()}}""")
        liveSst.remove(file)
        deletedSst += file
        maybeRotate()
    }

    /** Truncate hint (e.g., after offline repair). */
    @Synchronized
    fun truncate(note: String? = null) {
        val ts = now()
        val n = note?.let { "\"note\":\"${esc(it)}\"," } ?: ""
        append("""{"type":"Truncate",$n"ts":$ts}""")
        maybeRotate()
    }

    /** Format bump (for readers to gate on). */
    @Synchronized
    fun formatBump(newMajor: Int, newMinor: Int) {
        append("""{"type":"FormatBump","major":$newMajor,"minor":$newMinor,"ts":${now()}}""")
        maybeRotate()
    }

    /** Snapshot for planning/monitoring. */
    @Synchronized
    fun snapshot(): Snapshot = Snapshot(
        stripesWritten = stripesWritten,
        liveSst = liveSst.toList(),
        deletedSst = deletedSst.toList(),
        lastSstSeal = lastSstSeal,
        lastCheckpoint = lastCheckpoint
    )

    /**
     * Stop fast-mode flusher and close channel, performing a final strong sync.
     */
    @Synchronized
    override fun close() {
        if (!fastMode) return
        val t = flusherThread
        running.set(false)
        if (t != null && t.isAlive) t.join()
        fastCh?.let { ch ->
            try {
                ch.force(true)
            } finally {
                ch.close()
            }
        }
        fastCh = null
        flusherThread = null
    }

    fun fsync() = fastCh?.force(false)

    /* ─────────── Append path (mode-aware) ─────────── */

    private fun append(line: String) {
        if (fastMode) q.add(line) else appendNow(line)
    }

    // Durable mode: open→write→force(true)→close
    private fun appendNow(line: String) {
        Files.createDirectories(path.parent)
        FileChannel.open(path, CREATE, WRITE, APPEND).use { ch ->
            writeOne(ch, line)
            ch.force(true) // data+metadata
        }
        maybeRotate()
    }

    /* ─────────── Flusher (Fast mode) ─────────── */

    private fun runFlusher() {
        val ch = fastCh ?: return
        val batchBufs = ArrayList<ByteBuffer>(batchMaxEvents)
        var lastStrong = lastStrongSyncNanos
        val strongNs = strongSyncIntervalMillis * 1_000_000L

        while (running.get() || q.isNotEmpty()) {
            batchBufs.clear()
            var n = 0
            val start = System.nanoTime()
            while (n < batchMaxEvents) {
                val s = q.poll() ?: break
                batchBufs += encodeRecord(s)
                n++
                if ((System.nanoTime() - start) > batchMaxMicros * 1_000L) break
            }

            if (batchBufs.isEmpty()) {
                Thread.sleep(1)
            } else {
                for (b in batchBufs) while (b.hasRemaining()) ch.write(b)
                ch.force(false) // fdatasync-ish
                maybeRotate(ch)
            }

            val now = System.nanoTime()
            if (now - lastStrong >= strongNs) {
                ch.force(true)
                lastStrong = now
            }
        }

        // final drain
        val tail = mutableListOf<ByteBuffer>()
        while (true) {
            val s = q.poll() ?: break
            tail += encodeRecord(s)
        }
        for (b in tail) while (b.hasRemaining()) ch.write(b)
        ch.force(true)
        maybeRotate(ch)
    }

    /* ─────────── Rotation ─────────── */

    private fun maybeRotate(ch: FileChannel? = fastCh) {
        if (rotateMaxBytes <= 0) return
        val curSize = try {
            ch?.size() ?: Files.size(path)
        } catch (_: Throwable) {
            0L
        }
        if (curSize < rotateMaxBytes) return

        // Roll to MANIFEST.yyyyMMdd-HHmmss.nnn
        val ts = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
            .format(java.time.LocalDateTime.now())
        val rolled = path.resolveSibling("MANIFEST.$ts")
        try {
            ch?.force(true)
        } catch (_: Throwable) {
        }
        ch?.close()
        Files.move(path, rolled)
        // write a small header into the new file
        FileChannel.open(path, CREATE, WRITE).use { fc ->
            val hdr = """{"type":"Rotate","from":"${esc(rolled.fileName.toString())}","ts":${now()}}"""
            writeOne(fc, hdr)
            fc.force(true)
        }
        // reopen for fast mode
        if (fastMode) {
            fastCh = FileChannel.open(path, CREATE, WRITE, APPEND)
        }
    }

    /* ─────────── Encoding helpers ─────────── */

    private fun encodeRecord(line: String): ByteBuffer {
        val data = line.toByteArray(UTF_8)
        val buf = ByteBuffer.allocate(4 + data.size + 4).order(ByteOrder.LITTLE_ENDIAN)
        buf.putInt(data.size)
        buf.put(data)
        val crc = CRC32C().apply { update(data) }.value.toInt()
        buf.putInt(crc)
        buf.flip()
        return buf
    }

    private fun writeOne(ch: FileChannel, line: String) {
        val buf = encodeRecord(line)
        while (buf.hasRemaining()) ch.write(buf)
    }

    /* ─────────── Replay helpers ─────────── */

    private fun applyEventLine(line0: String) {
        val i = line0.indexOf('{')
        val line = if (i >= 0) line0.substring(i).trim() else line0.trim()
        when (extract(line, "type")) {
            "StripeCommit" -> {
                extract(line, "after")?.toLongOrNull()?.let { if (it >= stripesWritten) stripesWritten = it }
            }

            "SSTSeal" -> {
                val e = SstSealEvent(
                    level = requireInt(line, "level"),
                    file = requireString(line, "file"),
                    entries = requireLong(line, "entries"),
                    firstKeyHex = extract(line, "firstKeyHex"),
                    lastKeyHex = extract(line, "lastKeyHex"),
                    ts = requireLong(line, "ts")
                )
                sstSeals += e
                lastSstSeal = e
                liveSst += e.file
                deletedSst.remove(e.file)
            }

            "SSTDelete" -> {
                val f = requireString(line, "file")
                liveSst.remove(f); deletedSst += f
            }

            "CompactionStart" -> {
                // purely informational; nothing to track
            }

            "CompactionEnd" -> {
                val output = requireString(line, "output")
                liveSst += output
                // inputs may be absent if older record; be tolerant
                val ins = extractArray(line, "inputs")
                ins.forEach { f -> liveSst.remove(f); deletedSst += f }
            }

            "Checkpoint" -> {
                lastCheckpoint = CheckpointEvent(
                    name = extract(line, "name"),
                    stripe = extract(line, "stripe")?.toLongOrNull(),
                    lastSeq = extract(line, "lastSeq")?.toLongOrNull(),
                    ts = extract(line, "ts")?.toLongOrNull() ?: 0L
                )
            }

            "Rotate", "Truncate", "FormatBump" -> {
                // no-op for in-memory state
            }
        }
    }

    private fun extract(json: String, key: String): String? {
        val pattern =
            """"${Regex.escape(key)}"\s*:\s*(?:
            "((?:\\.|[^"\\])*)"
          | ([^,\}\s][^,\}]*)
        )""".replace("\n", "")
        val re = Regex(pattern)
        val m = re.find(json) ?: return null
        val s = m.groups[1]?.value ?: m.groups[2]?.value ?: return null
        return s
    }

    private fun extractArray(json: String, key: String): List<String> {
        val re = Regex(""""${Regex.escape(key)}"\s*:\s*\[(.*?)]""")
        val m = re.find(json) ?: return emptyList()
        val body = m.groupValues[1]
        if (body.isBlank()) return emptyList()
        return body.split(',')
            .map { it.trim().trim('"') }
            .filter { it.isNotBlank() }
    }

    private fun requireInt(line: String, key: String): Int =
        extract(line, key)?.trim()?.toIntOrNull()
            ?: error("manifest parse error: key=$key (int) :: $line")

    private fun requireLong(line: String, key: String): Long =
        extract(line, key)?.trim()?.toLongOrNull()
            ?: error("manifest parse error: key=$key (long) :: $line")

    private fun requireString(line: String, key: String): String =
        extract(line, key)?.trim()
            ?: error("manifest parse error: key=$key (string) :: $line")

    private fun esc(s: String) = s
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")

    private fun readFully(ch: FileChannel, buf: ByteBuffer): Boolean {
        while (buf.hasRemaining()) {
            val n = ch.read(buf)
            if (n < 0) return false
        }
        return true
    }

    private fun now() = System.currentTimeMillis()
}
