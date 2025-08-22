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

    @Synchronized
    fun advance(newCount: Long) {
        require(newCount >= stripesWritten) { "stripe counter must be monotonic" }
        stripesWritten = newCount
        append("""{"type":"StripeCommit","after":$newCount,"ts":${now()}}""")
    }

    @Synchronized
    fun sstSeal(level: Int, file: String, entries: Long, firstKeyHex: String?, lastKeyHex: String?) {
        val ts = now()
        val e = SstSealEvent(level, file, entries, firstKeyHex, lastKeyHex, ts)

        append(buildString {
            append("{")
            append("\"type\":\"SSTSeal\",")
            append("\"level\":").append(level).append(",")
            append("\"file\":\"").append(esc(file)).append("\",")
            append("\"entries\":").append(entries).append(",")

            if (firstKeyHex != null) {
                append("\"firstKeyHex\":\"").append(esc(firstKeyHex)).append("\",")
            }
            if (lastKeyHex != null) {
                append("\"lastKeyHex\":\"").append(esc(lastKeyHex)).append("\",")
            }

            append("\"ts\":").append(ts).append("}")
        })

        sstSeals += e
        lastSstSeal = e
    }

    @Synchronized
    fun checkpoint(name: String? = null, stripe: Long? = null, lastSeq: Long? = null) {
        val ts = now()
        append(buildString {
            append("{")
            append("\"type\":\"Checkpoint\",")
            if (name != null) append("\"name\":\"").append(esc(name)).append("\",")
            if (stripe != null) append("\"stripe\":").append(stripe).append(",")
            if (lastSeq != null) append("\"lastSeq\":").append(lastSeq).append(",")
            append("\"ts\":").append(ts).append("}")
        })
        lastCheckpoint = CheckpointEvent(name, stripe, lastSeq, ts)
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
                .replace(Regex("""\s+#.*?(?=["|(|\[])"""), "")

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