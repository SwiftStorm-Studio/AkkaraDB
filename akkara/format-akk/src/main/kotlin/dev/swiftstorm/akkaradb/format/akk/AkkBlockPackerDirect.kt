package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.MAX_RECORD
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.zip.CRC32C

class AkkBlockPackerDirect(
    private val onBlockReady: (ByteBuffer) -> Unit,
    private val pool: BufferPool = Pools.io()
) : BlockPacker, Closeable {

    override val blockSize: Int = BLOCK_SIZE

    /* ---- per-instance state ---- */
    private val scratch: ByteBuffer = pool.get()
    private val crc = CRC32C()

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

        // 1. allocate a fresh 32KiB block from the pool
        val blk = pool.get()

        // 2. [len][payload]--------------------------------------
        blk.putInt(payloadLen)
        blk.put(scratch)

        // 3. zero‑pad up to PAYLOAD_LIMIT (32KiB − 4) for deterministic checksum
        while (blk.position() < PAYLOAD_LIMIT + 4) blk.put(0)

        // 4. CRC32C on the first (4+payloadLen) bytes
        crc.reset()
        val dup = blk.duplicate()
        dup.limit(4 + payloadLen).position(0)
        crc.update(dup)
        blk.position(PAYLOAD_LIMIT + 4).putInt(crc.value.toInt())
        blk.flip()

        // 5. HAND‑OFF — Duplicate as read‑only for downstream; keep the original
        //    so we control its lifecycle.
        val ro = blk.asReadOnlyBuffer()
        onBlockReady(ro)        // StripeWriter.addBlock(ro)

        scratch.clear()         // ready for next pack cycle
    }

}
