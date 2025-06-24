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
    private val pool: BufferPool = Pools.io(),   // 既存の共通プールを利用
    initCap: Int = 32 * 1024
) : Closeable {

    private val ch = FileChannel.open(path, WRITE, CREATE, APPEND, DSYNC)

    private var scratch: ByteBuffer = pool.get(initCap)

    /* ---------- public ---------- */

    fun append(record: ByteBuffer) =
        writeRecord(WalRecord.Add(record.asReadOnlyBuffer()))

    fun sealSegment() = writeRecord(WalRecord.Seal)

    fun checkpoint(stripeIdx: Long, seqNo: Long) =
        writeRecord(WalRecord.CheckPoint(stripeIdx, seqNo))

    /* ---------- core ---------- */

    private fun writeRecord(r: WalRecord) {
        while (true) {
            scratch.clear()
            try {
                r.writeTo(scratch)
                scratch.flip()
                while (scratch.hasRemaining()) ch.write(scratch)
                return                               // success
            } catch (_: BufferOverflowException) {
                // 乗り切らなかった：倍サイズをプールから借り直す
                val newCap = scratch.capacity() * 2
                pool.release(scratch)               // 旧バッファ返却
                scratch = pool.get(newCap)
            }
        }
    }

    /* ---------- lifecycle ---------- */

    override fun close() {
        pool.release(scratch)
        ch.close()
    }
}
