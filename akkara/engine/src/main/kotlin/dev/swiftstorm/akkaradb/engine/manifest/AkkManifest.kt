package dev.swiftstorm.akkaradb.engine.manifest

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

class AkkManifest(private val path: Path) {
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
        val ts: Long
    )

    fun load() {
        if (!Files.exists(path)) return
        Files.readAllLines(path, UTF_8)
            .map(String::trim)
            .filter(String::isNotEmpty)
            .forEach(::applyEventLine)
    }

    @Synchronized
    fun advance(newCount: Long) {
        require(newCount >= stripesWritten) { "stripe counter must be monotonic" }
        stripesWritten = newCount
        append("""{"type":"StripeCommit","after":$newCount,"ts":${now()}}""")
    }

    @Synchronized
    fun sstSeal(level: Int, file: String, entries: Long, firstKeyHex: String?, lastKeyHex: String?) {
        append(buildString {
            append("""{"type":"SSTSeal","level":$level,"file":"${esc(file)}","entries":$entries,""")
            firstKeyHex?.let { append(""""firstKeyHex":"${esc(it)}",""") }
            lastKeyHex?.let { append(""""lastKeyHex":"${esc(it)}",""") }
            append(""""ts":${now()}}""")
        })
    }

    @Synchronized
    fun checkpoint(name: String? = null, stripe: Long? = null, lastSeq: Long? = null) {
        append(buildString {
            append("""{"type":"Checkpoint",""")
            name?.let { append(""""name":"${esc(it)}",""") }
            stripe?.let { append(""""stripe":$it,""") }
            lastSeq?.let { append(""""lastSeq":$it,""") }
            append(""""ts":${now()}}""")
        })
    }

    private fun append(line: String) {
        Files.createDirectories(path.parent)
        FileChannel.open(path, CREATE, WRITE, APPEND).use { ch ->
            val buf = ByteBuffer.wrap((line + "\n").toByteArray(UTF_8))
            while (buf.hasRemaining()) ch.write(buf)
            ch.force(true)
        }
    }

    private fun applyEventLine(line: String) {
        when (extract(line, "type")) {
            "StripeCommit" -> {
                extract(line, "after")?.toLongOrNull()?.let {
                    if (it >= stripesWritten) stripesWritten = it
                }
            }

            "SSTSeal" -> sstSeals += SstSealEvent(
                level = extract(line, "level")!!.toInt(),
                file = extract(line, "file")!!,
                entries = extract(line, "entries")!!.toLong(),
                firstKeyHex = extract(line, "firstKeyHex"),
                lastKeyHex = extract(line, "lastKeyHex"),
                ts = extract(line, "ts")!!.toLong()
            )

            "Checkpoint" -> {
                lastCheckpoint = CheckpointEvent(
                    name = extract(line, "name"),
                    ts = extract(line, "ts")?.toLongOrNull() ?: 0L
                )
            }
        }
    }

    private fun extract(json: String, key: String): String? =
        Regex(""""${Regex.escape(key)}"\s*:\s*("?)(.*?)\1""")
            .find(json)?.groupValues?.getOrNull(2)

    private fun esc(s: String) = s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")

    private fun now() = System.currentTimeMillis()
}
