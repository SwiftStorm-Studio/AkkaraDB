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
 * Block layout (endianness fixed to LITTLE_ENDIAN):
 *   [0..3]       payloadLen: Int (bytes in payload area)
 *   [4..4+N)     payload bytes (sequence of [u32 len][bytes] records)
 *   [..]         zero padding up to PAYLOAD_LIMIT (for determinism/portability)
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

    private val scratch: ByteBuffer = pool.get()
    private val crc = CRC32C()
    private val zeroPadChunk = ZERO_PAD_CHUNK

    override fun addRecord(record: ByteBuffer) {
        val recLen = record.remaining()
        require(recLen <= MAX_RECORD) { "Record $recLen B exceeds $MAX_RECORD B" }

        if (scratch.position() + 4 + recLen > PAYLOAD_LIMIT) emitBlock()

        // [u32 recLen][rec bytes] を LE で
        scratch.putInt(recLen)
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

        // 32 KiB block, LE 明示
        val blk = pool.get()
        blk.clear()
        blk.order(ByteOrder.LITTLE_ENDIAN)

        // [0..3] payloadLen
        blk.putInt(payloadLen)

        // [4..4+N) payload
        blk.put(scratch)

        // zero padding to fixed size payload area
        var pad = PAYLOAD_LIMIT - payloadLen
        while (pad > 0) {
            val n = min(pad, ZERO_PAD_CHUNK.capacity())
            val dup = ZERO_PAD_CHUNK.duplicate().apply {
                position(0); limit(n)
            }
            blk.put(dup)
            pad -= n
        }
        // position == 4 + PAYLOAD_LIMIT

        // CRC32C over [0 .. 4+payloadLen)
        crc.reset()
        val crcRegion = blk.duplicate().apply {
            position(0); limit(4 + payloadLen)
        }
        crc.update(crcRegion)

        blk.putInt(crc.value.toInt())

        blk.flip()
        onBlockReady(blk)

        scratch.clear()
    }

    companion object {
        private val ZERO_PAD_CHUNK: ByteBuffer =
            ByteBuffer.allocateDirect(4096).order(ByteOrder.LITTLE_ENDIAN).apply { clear() }.asReadOnlyBuffer()
    }
}