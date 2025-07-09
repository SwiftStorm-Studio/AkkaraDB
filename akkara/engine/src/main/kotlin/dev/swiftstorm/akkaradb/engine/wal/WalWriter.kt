package dev.swiftstorm.akkaradb.engine.wal

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import java.io.Closeable
import java.nio.BufferOverflowException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*

class WalWriter(
    path: Path,
    private val pool: BufferPool = Pools.io(),
    initCap: Int = 32 * 1024
) : Closeable {

    private val ch = FileChannel.open(path, WRITE, CREATE, APPEND, DSYNC)

    /** Reusable scratch buffer (Direct) */
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
        while (true) {
            scratch.clear()
            try {
                r.writeTo(scratch)
                break                                   // success
            } catch (_: BufferOverflowException) {
                // need bigger buffer (cap ≤ 1 MiB to avoid runaway)
                val needed = (scratch.capacity() * 2)
                    .coerceAtLeast(scratch.position() * 2)
                    .coerceAtMost(1 shl 20)
                pool.release(scratch)
                scratch = pool.get(needed)
            }
        }

        scratch.flip()
        while (scratch.hasRemaining()) ch.write(scratch)

        if (force) ch.force(true)                      // durability barrier
    }

    /* ---------- lifecycle ---------- */

    override fun close() {
        pool.release(scratch)
        ch.close()
    }
}
