package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
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
fun replayWal(path: Path, mem: MemTable, startStripe: Long = 0, startSeq: Long = Long.MIN_VALUE) {
    if (!Files.exists(path)) return

    FileChannel.open(path, READ).use { ch ->
        val buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size())
        var state = SegState.APPLY
        var offset = 0

        // First pass: locate offset after last checkpoint >= startStripe/startSeq
        while (buf.hasRemaining()) {
            val pos = buf.position()
            val rec = try {
                WalRecord.readFrom(ByteBufferL.wrap(buf))
            } catch (_: Throwable) {
                buf.position(pos); break
            }
            when (rec) {
                WalRecord.Seal -> state = SegState.SEALED
                is WalRecord.CheckPoint -> {
                    if (state != SegState.SEALED) {
                        error("WAL corrupted: CheckPoint without preceding Seal @ pos=${buf.position()}")
                    }
                    state = SegState.APPLY
                    if (rec.stripeIdx > startStripe || (rec.stripeIdx == startStripe && rec.seqNo >= startSeq)) {
                        offset = buf.position()
                    }
                }
                else -> {}
            }
        }

        buf.position(offset)
        state = SegState.APPLY

        // Second pass: apply remaining records
        while (buf.hasRemaining()) {
            val pos = buf.position()
            val rec = try {
                WalRecord.readFrom(ByteBufferL.wrap(buf))
            } catch (_: Throwable) {
                buf.position(pos); break
            }
            when (rec) {
                is WalRecord.Add -> {
                    if (state == SegState.SEALED) {
                        error("WAL corrupted: Add encountered after Seal but before CheckPoint @ pos=${buf.position()}")
                    }
                    val kv = AkkRecordReader.read(rec.payload.asReadOnly())
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
