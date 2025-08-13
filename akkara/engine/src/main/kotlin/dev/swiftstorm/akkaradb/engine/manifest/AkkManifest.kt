package dev.swiftstorm.akkaradb.engine.manifest

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

/**
 * Append-only manifest storing events in JSON Lines (JSONL) format.
 *
 * Events:
 *  - StripeCommit: {"type":"StripeCommit","after":<long>,"ts":<epoch_ms>}
 *  - SSTSeal: {"type":"SSTSeal","level":<int>,"file":"...","entries":<long>, ...}
 *  - Checkpoint: {"type":"Checkpoint","name":"...","ts":<epoch_ms>}
 *
 * All writes are followed by fsync for durability. Thread-safe.
 */
class AkkManifest(private val path: Path) {

    @Volatile
    var stripesWritten: Long = 0L
        private set

    /** Replay events from disk to restore state. */
    fun load() {
        if (!Files.exists(path)) return
        Files.readAllLines(path, UTF_8)
            .asSequence()
            .map(String::trim)
            .filter(String::isNotEmpty)
            .forEach(::applyEventLine)
    }

    /** Append a StripeCommit event. */
    @Synchronized
    fun advance(newCount: Long) {
        require(newCount >= stripesWritten) { "stripe counter must be monotonic" }
        stripesWritten = newCount
        appendJson("{\"type\":\"StripeCommit\",\"after\":$newCount,\"ts\":${System.currentTimeMillis()}}\n")
    }

    /** Append an SSTSeal event. */
    @Synchronized
    fun sstSeal(level: Int, file: String, entries: Long, firstKeyHex: String?, lastKeyHex: String?) {
        val fk = firstKeyHex?.let { "\"firstKeyHex\":\"${escape(it)}\"," } ?: ""
        val lk = lastKeyHex?.let { "\"lastKeyHex\":\"${escape(it)}\"," } ?: ""
        appendJson("{\"type\":\"SSTSeal\",\"level\":$level,\"file\":\"${escape(file)}\",\"entries\":$entries,$fk$lk\"ts\":${System.currentTimeMillis()}}\n")
    }

    /** Append a Checkpoint event. */
    @Synchronized
    fun checkpoint(name: String? = null) {
        val nm = name?.let { "\"name\":\"${escape(it)}\"," } ?: ""
        appendJson("{\"type\":\"Checkpoint\",$nm\"ts\":${System.currentTimeMillis()}}\n")
    }

    private fun appendJson(line: String) {
        Files.createDirectories(path.parent)
        FileChannel.open(path, CREATE, WRITE, APPEND).use { ch ->
            val buf = ByteBuffer.wrap(line.toByteArray(UTF_8))
            while (buf.hasRemaining()) ch.write(buf)
            ch.force(true)
        }
    }

    private fun applyEventLine(line: String) {
        when (extractString(line, "type")) {
            "StripeCommit" -> extractLong(line, "after")?.let { if (it >= stripesWritten) stripesWritten = it }
        }
    }

    private fun extractString(json: String, key: String): String? {
        val pat = Regex("\"" + Regex.escape(key) + "\"\\s*:\\s*\"(.*?)\"")
        return pat.find(json)?.groupValues?.getOrNull(1)
    }

    private fun extractLong(json: String, key: String): Long? {
        val pat = Regex("\"" + Regex.escape(key) + "\"\\s*:\\s*([-]?[0-9]+)")
        return pat.find(json)?.groupValues?.getOrNull(1)?.toLongOrNull()
    }

    private fun escape(s: String): String = buildString(s.length + 8) {
        for (c in s) when (c) {
            '\\' -> append("\\\\")
            '"' -> append("\\\"")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            else -> append(c)
        }
    }
}
