package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.MAX_RECORD
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C
import kotlin.math.min

/**
 * Block packer that assembles fixed-size 32 KiB blocks.
 *
 * Block layout (portable, endianness fixed to BIG_ENDIAN):
 *   [0..3]     payloadLen: Int (bytes in payload area)
 *   [4..4+N)   payload bytes (sequence of [u32 len][bytes] records)
 *   [..]       zero padding up to PAYLOAD_LIMIT (for determinism/portability)
 *   [end-4..end) crc32c over bytes [0 .. 4+payloadLen)
 *
 * Caller passes each record via [addRecord]; when the payload area would overflow,
 * the current block is emitted to [onBlockReady].
 */
class AkkBlockPackerDirect(
    private val onBlockReady: (ByteBuffer) -> Unit,
    private val pool: BufferPool = Pools.io()
) : BlockPacker, Closeable {

    override val blockSize: Int = BLOCK_SIZE

    /* ---- per-instance state ---- */
    private val scratch: ByteBuffer = pool.get() // accumulates payload (<= PAYLOAD_LIMIT)
    private val crc = CRC32C()

    // Reusable zero buffer to avoid allocating on every emit
    private val zeroPadChunk = ZERO_PAD_CHUNK

    override fun addRecord(record: ByteBuffer) {
        require(record.remaining() <= MAX_RECORD) {
            "Record ${record.remaining()} B exceeds $MAX_RECORD B"
        }
        if (scratch.position() + 4 + record.remaining() > PAYLOAD_LIMIT) emitBlock()
        scratch.putInt(record.remaining())
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

        val blk = pool.get()             // 32 KiB direct buffer
        blk.clear()
        blk.order(ByteOrder.BIG_ENDIAN)  // make endianness explicit

        // [0..3] payloadLen
        blk.putInt(payloadLen)
        // [4..4+N) payload bytes
        blk.put(scratch)
        // zero padding to [4+payloadLen .. 4+PAYLOAD_LIMIT)
        var pad = PAYLOAD_LIMIT - payloadLen
        while (pad > 0) {
            val n = min(pad, zeroPadChunk.size)
            blk.put(zeroPadChunk, 0, n)
            pad -= n
        }
        // position is now PAYLOAD_LIMIT + 4

        // CRC32C over [0 .. 4+payloadLen)
        crc.reset()
        val crcRegion = blk.duplicate().apply {
            position(0)
            limit(4 + payloadLen)
        }
        crc.update(crcRegion)

        // trailing crc at end of block
        blk.putInt(crc.value.toInt())

        blk.flip()
        onBlockReady(blk)

        scratch.clear()
    }

    companion object {
        private val ZERO_PAD_CHUNK = ByteArray(4096) // shared zero buffer
    }
}
