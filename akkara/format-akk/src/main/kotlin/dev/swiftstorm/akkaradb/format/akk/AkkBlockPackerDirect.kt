package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Pools
import dev.swiftstorm.akkaradb.format.api.BlockPacker
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C
import kotlin.math.min

/**
 * Block packer that assembles fixed-size 32 KiB blocks (ALL Little-Endian).
 *
 * Block layout:
 *   [0..3]         payloadLen: u32 LE (bytes in payload area)
 *   [4..4+N)       payload bytes (sequence of [u32 LE len][bytes] records)
 *   [4+N..4+LIM)   zero padding up to PAYLOAD_LIMIT (determinism/portability)
 *   [end-4..end)   crc32c(u32 LE) over bytes [0 .. 4+payloadLen)
 *
 * Ownership: onBlockReady receives a pooled ByteBufferL (size=BLOCK_SIZE).
 * It MUST consume and release it.
 */
class AkkBlockPackerDirect(
    private val onBlockReady: (ByteBufferL) -> Unit,
    private val pool: BufferPool = Pools.io()
) : BlockPacker, Closeable {

    override val blockSize: Int = BLOCK_SIZE

    private val scratch: ByteBufferL = pool.get(PAYLOAD_LIMIT)
    private val crc = CRC32C()

    override fun addRecord(record: ByteBufferL) {
        val recLen = record.remaining
        val need = 4 + recLen // u32 len + rec bytes

        require(need <= PAYLOAD_LIMIT) {
            "Encoded record ($need B) exceeds payload limit $PAYLOAD_LIMIT"
        }

        if (scratch.position + need > PAYLOAD_LIMIT) {
            emitBlock()
        }

        // [u32 recLen][rec bytes]
        scratch.putInt(recLen)
        scratch.put(record.slice().asReadOnlyByteBuffer())
    }

    override fun flush() {
        if (scratch.position > 0) emitBlock()
    }

    override fun close() = pool.release(scratch)

    /* ---- internal ---- */
    private fun emitBlock() {
        require(BLOCK_SIZE == 4 + PAYLOAD_LIMIT + 4) {
            "BLOCK_SIZE ($BLOCK_SIZE) must equal 4 + PAYLOAD_LIMIT ($PAYLOAD_LIMIT) + 4"
        }

        val payloadLen = scratch.position
        scratch.flip()

        val blk = pool.get(BLOCK_SIZE)
        blk.clear()

        // [0..3] payloadLen
        blk.putInt(payloadLen)

        // [4..4+N) payload
        blk.put(scratch.asReadOnlyByteBuffer())

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

        // CRC32C over [0 .. 4+payloadLen)
        crc.reset()
        val crcRegion = blk.asReadOnlyByteBuffer().duplicate().apply {
            position(0); limit(4 + payloadLen)
        }
        crc.update(crcRegion)
        blk.putInt(crc.value.toInt())

        blk.flip()
        onBlockReady(blk) // NOTE: release responsibility is on the callback

        scratch.clear()
    }

    companion object {
        private val ZERO_PAD_CHUNK: ByteBuffer =
            ByteBuffer.allocateDirect(4096)
                .order(ByteOrder.LITTLE_ENDIAN) // MAKE SURE LE
                .apply { clear() }
                .asReadOnlyBuffer()
    }
}
