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

class AkkManifest(
    private val path: Path,
    val isFastMode: Boolean = true,
    // Fast mode tuning (N/T/S)
    private val batchMaxEvents: Int = 128,
    private val batchMaxMicros: Long = 500,     // ≈ N or T µs
    private val strongSyncIntervalMillis: Long = 1000
) : Closeable {

    /* ─────────── State (replayed by load) ─────────── */
    @Volatile
    var stripesWritten: Long = 0L
        private set

    @Volatile
    var lastSstSeal: SstSealEvent? = null
        private set

    @Volatile
    var lastCheckpoint: CheckpointEvent? = null
        private set

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

    /**
     * Loads and replays manifest into memory state.
     * Idempotent; safe to call multiple times.
     */
    fun load() {
        if (!Files.exists(path)) return
        FileChannel.open(path, READ).use { ch ->
            val lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
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
            }
        }
    }

    /**
     * Start fast-mode background flusher if needed.
     */
    @Synchronized
    fun start() {
        if (!isFastMode || running.get()) return
        Files.createDirectories(path.parent)
        // open once and reuse
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
        val json = """{"type":"StripeCommit","after":$newCount,"ts":${now()}}"""
        append(json)
    }

    /** Append SSTSeal(level,file,entries,firstKeyHex,lastKeyHex). */
    @Synchronized
    fun sstSeal(level: Int, file: String, entries: Long, firstKeyHex: String?, lastKeyHex: String?) {
        val ts = now()
        val e = SstSealEvent(level, file, entries, firstKeyHex, lastKeyHex, ts)

        val json = buildString {
            append("{")
            append("\"type\":\"SSTSeal\",")
            append("\"level\":").append(level).append(",")
            append("\"file\":\"").append(esc(file)).append("\",")
            append("\"entries\":").append(entries).append(",")
            if (firstKeyHex != null) append("\"firstKeyHex\":\"").append(esc(firstKeyHex)).append("\",")
            if (lastKeyHex != null) append("\"lastKeyHex\":\"").append(esc(lastKeyHex)).append("\",")
            append("\"ts\":").append(ts).append("}")
        }
        append(json)

        sstSeals += e
        lastSstSeal = e
    }

    /** Append Checkpoint(name,stripe,lastSeq). */
    @Synchronized
    fun checkpoint(name: String? = null, stripe: Long? = null, lastSeq: Long? = null) {
        val ts = now()
        val json = buildString {
            append("{")
            append("\"type\":\"Checkpoint\",")
            if (name != null) append("\"name\":\"").append(esc(name)).append("\",")
            if (stripe != null) append("\"stripe\":").append(stripe).append(",")
            if (lastSeq != null) append("\"lastSeq\":").append(lastSeq).append(",")
            append("\"ts\":").append(ts).append("}")
        }
        append(json)
        lastCheckpoint = CheckpointEvent(name, stripe, lastSeq, ts)
    }

    /**
     * Stop fast-mode flusher and close channel, performing a final strong sync.
     */
    @Synchronized
    override fun close() {
        if (!isFastMode) return
        val t = flusherThread
        running.set(false)
        // wake the flusher quickly
        if (t != null && t.isAlive) t.join()
        fastCh?.let { ch ->
            try {
                ch.force(true) // final strong sync
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
        if (isFastMode) {
            q.add(line)
        } else {
            appendNow(line) // durable synchronous mode
        }
    }

    // Durable mode: open→write→force(true)→close
    private fun appendNow(line: String) {
        Files.createDirectories(path.parent)
        FileChannel.open(path, CREATE, WRITE, APPEND).use { ch ->
            writeOne(ch, line)
            ch.force(true) // data+metadata
        }
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
            // Collect up to N or T µs
            while (n < batchMaxEvents) {
                val s = q.poll() ?: break
                batchBufs += encodeRecord(s)
                n++
                if ((System.nanoTime() - start) > batchMaxMicros * 1_000L) break
            }

            if (batchBufs.isEmpty()) {
                // tiny sleep to avoid busy wait; still very responsive
                Thread.sleep(1)
            } else {
                // write all
                for (b in batchBufs) {
                    while (b.hasRemaining()) ch.write(b)
                }
                // data-only sync (fdatasync)
                ch.force(false)
            }

            // Strong sync periodically
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
                extract(line, "after")?.toLongOrNull()?.let {
                    if (it >= stripesWritten) stripesWritten = it
                }
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
            }
            "Checkpoint" -> {
                lastCheckpoint = CheckpointEvent(
                    name = extract(line, "name"),
                    stripe = extract(line, "stripe")?.toLongOrNull(),
                    lastSeq = extract(line, "lastSeq")?.toLongOrNull(),
                    ts = extract(line, "ts")?.toLongOrNull() ?: 0L
                )
            }
        }
    }

    private fun extract(json: String, key: String): String? {
        val pattern =
            """"${Regex.escape(key)}"\s*:\s*(?:
            "((?:\\.|[^"\\])*)"
          | ([^,\}\s][^,\}]*)
        )"""
                .replace("\n", "")
        val re = Regex(pattern)
        val m = re.find(json) ?: return null
        val s = m.groups[1]?.value ?: m.groups[2]?.value ?: return null
        return s
    }

    private fun requireInt(line: String, key: String): Int {
        val raw = extract(line, key)?.trim()
            ?: error("manifest parse error: missing $key :: $line")
        return raw.toIntOrNull()
            ?: error("manifest parse error: key=$key not int :: $line")
    }

    private fun requireLong(line: String, key: String): Long {
        val raw = extract(line, key)?.trim()
            ?: error("manifest parse error: missing $key :: $line")
        return raw.toLongOrNull()
            ?: error("manifest parse error: key=$key not long :: $line")
    }

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
