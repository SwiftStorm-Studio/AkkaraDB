package dev.swiftstorm.akkaradb.engine.manifest

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.util.zip.CRC32C

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
        val stripe: Long?,
        val lastSeq: Long?,
        val ts: Long
    )

    fun load() {
        if (!Files.exists(path)) return
        FileChannel.open(path, READ).use { ch ->
            val lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
            while (true) {
                lenBuf.clear()
                if (ch.read(lenBuf) != 4) break
                lenBuf.flip()
                val len = lenBuf.int
                if (len <= 0) break
                val recBuf = ByteBuffer.allocate(len + 4).order(ByteOrder.LITTLE_ENDIAN)
                if (ch.read(recBuf) != len + 4) break
                recBuf.flip()
                val data = ByteArray(len)
                recBuf.get(data)
                val storedCrc = recBuf.int
                val crc = CRC32C().apply { update(data) }.value.toInt()
                if (crc != storedCrc) break
                val line = String(data, UTF_8).trim()
                if (line.isNotEmpty()) applyEventLine(line)
            }
        }
    }

    @Synchronized
    fun advance(newCount: Long) {
        require(newCount >= stripesWritten) { "stripe counter must be monotonic" }
        stripesWritten = newCount
        append("""{"type":"StripeCommit","after":$newCount,"ts":${now()}}""")
    }

    @Synchronized
    fun sstSeal(level: Int, file: String, entries: Long, firstKeyHex: String?, lastKeyHex: String?) {
        val e = SstSealEvent(level, file, entries, firstKeyHex, lastKeyHex, now())
        append(buildString {
            append("""{"type":"SSTSeal","level":$level,"file":"${esc(file)}","entries":$entries,"""")
            firstKeyHex?.let { append(""""firstKeyHex":"${esc(it)}","""") }
            lastKeyHex?.let { append(""""lastKeyHex":"${esc(it)}","""") }
            append(""""ts":${e.ts}}""")
        })
        sstSeals += e
        lastSstSeal = e
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
            val data = line.toByteArray(UTF_8)
            val buf = ByteBuffer.allocate(4 + data.size + 4).order(ByteOrder.LITTLE_ENDIAN)
            buf.putInt(data.size)
            buf.put(data)
            val crc = CRC32C().apply { update(data) }.value.toInt()
            buf.putInt(crc)
            buf.flip()
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

            "SSTSeal" -> {
                val e = SstSealEvent(
                    level = extract(line, "level")!!.toInt(),
                    file = extract(line, "file")!!,
                    entries = extract(line, "entries")!!.toLong(),
                    firstKeyHex = extract(line, "firstKeyHex"),
                    lastKeyHex = extract(line, "lastKeyHex"),
                    ts = extract(line, "ts")!!.toLong()
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
