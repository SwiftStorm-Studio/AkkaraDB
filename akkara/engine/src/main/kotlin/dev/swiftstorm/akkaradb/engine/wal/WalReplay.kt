package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.engine.memtable.MemTable
import dev.swiftstorm.akkaradb.format.akk.AkkRecordReader
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.READ
import kotlin.math.max

object WalReplay {
    data class Result(
        val applied: Long,
        val lastStripe: Long,
        val lastSeq: Long,
        val segmentsVisited: Int
    )

    @JvmStatic
    fun replay(
        dirOrFile: Path,
        mem: MemTable,
        prefix: String,
        startStripe: Long = 0L,
        startSeq: Long = Long.MIN_VALUE
    ): Result {
        return if (Files.isRegularFile(dirOrFile)) {
            val r = replayOne(dirOrFile, mem, startStripe, startSeq)
            Result(r.applied, r.lastStripe, r.lastSeq, segmentsVisited = 1)
        } else {
            val segs: List<Path> = Files.list(dirOrFile).use { s ->
                s.toList()
            }.filter { Files.isRegularFile(it) }
                .mapNotNull { p -> WalNaming.parseIndex(prefix, p.fileName.toString())?.let { idx -> idx to p } }
                .sortedBy { it.first }
                .map { it.second }

            var curStripe = startStripe
            var curSeq = startSeq
            var totalApplied = 0L

            for (p in segs) {
                val r = replayOne(p, mem, curStripe, curSeq)
                totalApplied += r.applied
                curStripe = max(curStripe, r.lastStripe)
                curSeq = max(curSeq, r.lastSeq)
            }
            Result(totalApplied, curStripe, curSeq, segmentsVisited = segs.size)
        }
    }

    // ──────────────────────────── per-file replay ────────────────────────────

    private data class OneResult(val applied: Long, val lastStripe: Long, val lastSeq: Long)

    private fun replayOne(path: Path, mem: MemTable, startStripe: Long, startSeq: Long): OneResult {
        if (!Files.exists(path) || Files.size(path) == 0L) {
            return OneResult(0, startStripe, startSeq)
        }

        FileChannel.open(path, READ).use { ch ->
            val buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, ch.size())

            var state = SegState.APPLY
            var safeOffset = 0
            var lastStripe = startStripe
            var lastSeq = startSeq

            while (buf.hasRemaining()) {
                val pos = buf.position()
                val rec = try {
                    WalRecord.readFrom(ByteBufferL.wrap(buf))
                } catch (_: Throwable) {
                    buf.position(pos)
                    break
                }
                when (rec) {
                    is WalRecord.Seal -> {
                        if (state == SegState.SEALED) error("WAL corrupted: consecutive Seal @ pos=$pos")
                        state = SegState.SEALED
                    }

                    is WalRecord.CheckPoint -> {
                        if (state != SegState.SEALED) {
                            if (pos != 0) error("WAL corrupted: CheckPoint without Seal @ pos=$pos")
                        }
                        state = SegState.APPLY
                        if (rec.stripeIdx > startStripe || (rec.stripeIdx == startStripe && rec.seqNo >= startSeq)) {
                            safeOffset = buf.position()
                            lastStripe = rec.stripeIdx
                            lastSeq = rec.seqNo
                        }
                    }

                    is WalRecord.Add -> {}
                }
            }

            buf.position(safeOffset)
            state = SegState.APPLY

            var applied = 0L
            while (buf.hasRemaining()) {
                val pos = buf.position()
                val rec = try {
                    WalRecord.readFrom(ByteBufferL.wrap(buf))
                } catch (_: Throwable) {
                    buf.position(pos)
                    break
                }
                when (rec) {
                    is WalRecord.Add -> {
                        if (state == SegState.SEALED) error("WAL corrupted: Add after Seal but before CheckPoint @ pos=$pos")
                        val kv = AkkRecordReader.read(rec.payload.asReadOnly())
                        mem.put(kv)
                        applied++
                    }

                    is WalRecord.Seal -> {
                        if (state == SegState.SEALED) error("WAL corrupted: consecutive Seal @ pos=$pos")
                        state = SegState.SEALED
                    }

                    is WalRecord.CheckPoint -> {
                        if (state != SegState.SEALED) error("WAL corrupted: CheckPoint without Seal @ pos=$pos")
                        state = SegState.APPLY
                        lastStripe = max(lastStripe, rec.stripeIdx)
                        lastSeq = max(lastSeq, rec.seqNo)
                    }
                }
            }

            return OneResult(applied, lastStripe, lastSeq)
        }
    }
}

private enum class SegState { APPLY, SEALED }