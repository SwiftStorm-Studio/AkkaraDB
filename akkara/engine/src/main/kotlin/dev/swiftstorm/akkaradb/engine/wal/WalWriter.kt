package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

class WalWriter(
    path: Path,
    private val pool: BufferPool = Pools.io(),
    initCap: Int = 32 * 1024
) : Closeable {

    companion object {
        private const val MAX_RECORD_BYTES = 1 shl 20       // 1 MiB
    }

    private val ch = FileChannel.open(path, WRITE, CREATE, APPEND)

    /** Reusable scratch buffer (direct) */
    private var scratch: ByteBuffer = pool.get(initCap)

    /* ---------- public API ---------- */

    fun append(record: ByteBuffer) =
        writeRecord(WalRecord.Add(record.asReadOnlyBuffer()))

    fun sealSegment() =
        writeRecord(WalRecord.Seal, force = true)

    fun checkpoint(stripeIdx: Long, seqNo: Long) =
        writeRecord(WalRecord.CheckPoint(stripeIdx, seqNo), force = true)

    /* ---------- core ---------- */

    private fun writeRecord(r: WalRecord, force: Boolean = false) {
        val estimated = r.estimateSize()
        require(estimated <= MAX_RECORD_BYTES) {
            "WAL record too large: $estimated bytes (limit ${MAX_RECORD_BYTES}B)"
        }

        if (scratch.capacity() < estimated) {
            pool.release(scratch)
            var newCap = scratch.capacity()
            while (newCap < estimated) newCap = newCap * 2
            scratch = pool.get(newCap)
        }

        scratch.clear()
        r.writeTo(scratch)
        scratch.flip()
        while (scratch.hasRemaining()) ch.write(scratch)

        if (force) ch.force(true)                        // durability barrier
    }

    /* ---------- lifecycle ---------- */

    override fun close() {
        pool.release(scratch)
        ch.close()
    }
}

private fun WalRecord.estimateSize(): Int =
    when (this) {
        is WalRecord.Seal -> 1                // tag only
        is WalRecord.CheckPoint -> 1 + 10 + 10      // tag + VarLong×2
        is WalRecord.Add -> 1 + 5 + payload.remaining()
        // tag + VarInt(len) + data
    }
