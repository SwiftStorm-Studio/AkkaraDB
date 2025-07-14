package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ

fun replayWal(path: Path, mem: MemTable) {
    if (!Files.exists(path)) return

    FileChannel.open(path, READ).use { ch ->
        val buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size())
        var state = SegState.APPLY

        while (buf.hasRemaining()) {
            when (val rec = WalRecord.readFrom(buf)) {
                is WalRecord.Add -> {
                    if (state == SegState.SEALED) {
                        error("WAL corrupted: Add encountered after Seal but before CheckPoint @ pos=${buf.position()}")
                    }
                    // apply
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