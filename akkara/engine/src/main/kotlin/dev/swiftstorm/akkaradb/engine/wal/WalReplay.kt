package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

/**
 * Replays a WAL file into the given MemTable.
 *
 * Rules:
 *  - Records are applied in order.
 *  - A Seal must be followed by a CheckPoint before further Add records.
 *  - A torn/truncated tail (e.g., crash mid-record) is treated as EOF
 *    and replay stops cleanly without throwing.
 */
fun replayWal(path: Path, mem: MemTable) {
    if (!Files.exists(path)) return

    FileChannel.open(path, READ).use { ch ->
        val buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size())
        var state = SegState.APPLY

        while (buf.hasRemaining()) {
            val pos = buf.position()
            val rec = try {
                WalRecord.readFrom(buf) // validates lengths; ADD also validates CRC32C
            } catch (_: Throwable) {
                // Treat a torn/truncated tail as EOF and stop replay.
                // We rewind to the start of the bad record for clarity and exit.
                buf.position(pos)
                break
            }

            when (rec) {
                is WalRecord.Add -> {
                    if (state == SegState.SEALED) {
                        error("WAL corrupted: Add encountered after Seal but before CheckPoint @ pos=${buf.position()}")
                    }
                    // Apply to memtable
                    val kv = AkkRecordReader.read(rec.payload.asReadOnlyBuffer())
                    mem.put(kv)
                }

                WalRecord.Seal -> {
                    if (state == SegState.SEALED) {
                        error("WAL corrupted: consecutive Seal records @ pos=${buf.position()}")
                    }
                    state = SegState.SEALED
                }

                is WalRecord.CheckPoint -> {
                    if (state != SegState.SEALED) {
                        error("WAL corrupted: CheckPoint without preceding Seal @ pos=${buf.position()}")
                    }
                    state = SegState.APPLY
                }
            }
        }
    }
}

enum class SegState { APPLY, SEALED }
