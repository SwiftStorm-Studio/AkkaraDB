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
        var apply = true
        while (buf.hasRemaining()) {
            when (val r = WalRecord.readFrom(buf)) {
                is WalRecord.Add -> if (apply) {
                    val rec = AkkRecordReader.read(r.payload.duplicate())
                    mem.put(rec)
                }

                WalRecord.Seal -> apply = false   // ← skip after Seal
                is WalRecord.CheckPoint -> apply = true // ← resume after checkpoint
            }
        }
    }
}
