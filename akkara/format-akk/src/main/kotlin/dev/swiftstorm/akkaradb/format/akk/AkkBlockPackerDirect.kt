package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.MAX_RECORD
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * Thread-safe (per-instance) block packer.
 *
 * * scratch / CRC / pool Buffer は **インスタンス固有**なので競合なし
 * * 完成した 32 KiB ブロックは ctor で渡された [onBlockReady] に渡す
 */
class AkkBlockPackerDirect(
    private val onBlockReady: (ByteBuffer) -> Unit,
    private val pool: BufferPool = Pools.io()
) : BlockPacker, Closeable {

    override val blockSize: Int = BLOCK_SIZE

    /* ---- per-instance state ---- */
    private val scratch: ByteBuffer = pool.get()
    private val crc = CRC32()

    override fun addRecord(record: ByteBuffer) {
        require(record.remaining() <= MAX_RECORD) {
            "Record ${record.remaining()} B exceeds $MAX_RECORD B"
        }
        if (scratch.position() + record.remaining() > PAYLOAD_LIMIT) emitBlock()
        scratch.put(record.duplicate())
    }

    override fun flush() {
        if (scratch.position() > 0) emitBlock()
    }

    override fun close() = pool.release(scratch)

    /* ---- internal ---- */
    private fun emitBlock() {
        val payloadLen = scratch.position()
        scratch.flip()

        val blk = pool.get()
        blk.putInt(payloadLen)
        blk.put(scratch)
        while (blk.position() < PAYLOAD_LIMIT + 4) blk.put(0)

        crc.reset()
        blk.duplicate().limit(4 + payloadLen).position(0).let(crc::update)
        blk.position(PAYLOAD_LIMIT + 4).putInt(crc.value.toInt())
        blk.flip()

        onBlockReady(blk.asReadOnlyBuffer())
        pool.release(blk)
        scratch.clear()
    }
}
