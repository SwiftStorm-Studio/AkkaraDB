package dev.swiftstorm.akkaradb.debugger.core.inspectors

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.debugger.core.WALEntry
import dev.swiftstorm.akkaradb.debugger.core.WALReport
import dev.swiftstorm.akkaradb.engine.wal.WalFraming
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.fileSize
import kotlin.io.path.pathString

class WALInspector(private val path: Path) {

    fun inspect(): WALReport {
        val errors = mutableListOf<String>()
        val entries = mutableListOf<WALEntry>()
        var truncatedTail = false

        FileChannel.open(path, StandardOpenOption.READ).use { ch ->
            val fileSize = path.fileSize()

            if (fileSize == 0L) {
                return WALReport(
                    filePath = path.pathString,
                    fileSize = 0,
                    entries = emptyList(),
                    truncatedTail = false,
                    errors = listOf("WAL file is empty")
                )
            }

            val mapped = ch.map(FileChannel.MapMode.READ_ONLY, 0, fileSize)
            var offset = 0L
            var lsn = 0L

            while (mapped.hasRemaining()) {
                val entryOffset = offset

                try {
                    val payload = WalFraming.readOne(mapped)

                    if (payload == null) {
                        truncatedTail = true
                        break
                    }

                    // Parse payload as AKHdr32 + key + value
                    val payloadL = ByteBufferL.wrap(payload)
                    val entry = parseWALPayload(payloadL, entryOffset, lsn)
                    entries.add(entry)

                    offset = mapped.position().toLong()
                    lsn++

                } catch (e: Exception) {
                    errors.add("Entry at offset $entryOffset error: ${e.message}")
                    truncatedTail = true
                    break
                }
            }
        }

        return WALReport(
            filePath = path.pathString,
            fileSize = path.fileSize(),
            entries = entries,
            truncatedTail = truncatedTail,
            errors = errors
        )
    }

    private fun parseWALPayload(buf: ByteBufferL, offset: Long, lsn: Long): WALEntry {
        buf.position(0)

        // Read AKHdr32
        val kLen = buf.i16.toInt() and 0xFFFF
        val vLen = buf.i32
        val seq = buf.i64
        val flags = buf.i8.toInt()
        buf.position(buf.position + 1) // skip pad0
        val keyFP64 = buf.i64
        val miniKey = buf.i64

        val isTombstone = (flags and 0x01) != 0
        val operation = if (isTombstone) "DELETE" else "ADD"

        // Read key
        val keyBytes = ByteArray(kLen)
        repeat(kLen) { keyBytes[it] = buf.i8.toByte() }

        // Read value (if not tombstone)
        val valueBytes = if (!isTombstone && vLen > 0) {
            ByteArray(vLen).also { arr ->
                repeat(vLen) { arr[it] = buf.i8.toByte() }
            }
        } else null

        return WALEntry(
            offset = offset,
            lsn = lsn,
            payloadLen = 32 + kLen + (valueBytes?.size ?: 0),
            crcValid = true, // Already validated by WalFraming
            operation = operation,
            seq = seq,
            keyLen = kLen,
            valueLen = vLen,
            keyPreview = keyBytes.toHexString(),
            valuePreview = valueBytes?.toHexString()
        )
    }
}

private fun ByteArray.toHexString(): String =
    joinToString("") { "%02x".format(it) }