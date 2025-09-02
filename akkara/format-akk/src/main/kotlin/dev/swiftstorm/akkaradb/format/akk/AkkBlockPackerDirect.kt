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
 * Layout:
 *   [0..3]         payloadLen: u32 LE (bytes in payload area)
 *   [4..4+N)       payload bytes (sequence of [u32 LE len][bytes] records)
 *   [4+N..4+LIM)   zero padding up to PAYLOAD_LIMIT
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

    /** Payload staging buffer (LE, capacity = PAYLOAD_LIMIT). */
    private val scratch: ByteBufferL = pool.get(PAYLOAD_LIMIT)
    private val crc = CRC32C()

    /** Reusable 4-byte LE header for crc/update & write. */
    private val lenHeader: ByteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

    override fun addRecord(record: ByteBufferL) {
        val recLen = record.remaining
        val need = 4 + recLen // u32 len + rec bytes

        require(need <= PAYLOAD_LIMIT) {
            "Encoded record (${need}B) exceeds payload limit ($PAYLOAD_LIMIT)"
        }

        // overflow → flush current block first
        if (scratch.position + need > PAYLOAD_LIMIT) {
            emitBlock()
        }

        // write [u32 recLen][rec bytes]
        scratch.putInt(recLen)
        // avoid touching caller's position/limit
        scratch.put(record.asReadOnlyByteBuffer().slice())
    }

    override fun flush() {
        if (scratch.position > 0) emitBlock()
    }

    override fun close() {
        // packer is a staging object; flushing on close is often dangerous
        // (callers should control flush timing). Keep current semantics.
        pool.release(scratch)
    }

    /* ───────────────── internal ───────────────── */

    private fun emitBlock() {
        require(BLOCK_SIZE == 4 + PAYLOAD_LIMIT + 4) {
            "BLOCK_SIZE ($BLOCK_SIZE) must equal 4 + PAYLOAD_LIMIT ($PAYLOAD_LIMIT) + 4"
        }

        val payloadLen = scratch.position
        // Prepare scratch for read
        scratch.flip()

        // Precompute CRC over [payloadLen header(4B)] + [payload bytes]
        // 1) header
        lenHeader.clear()
        lenHeader.putInt(payloadLen)
        lenHeader.flip()

        crc.reset()
        crc.update(lenHeader.duplicate())                       // [0..3]  payloadLen
        crc.update(scratch.asReadOnlyByteBuffer().duplicate())  // [4..4+N) payload

        val blk = pool.get(BLOCK_SIZE)
        try {
            blk.clear()

            // [0..3] payloadLen
            blk.put(lenHeader.duplicate())

            // [4..4+N) payload
            blk.put(scratch.asReadOnlyByteBuffer())

            // [4+N..4+LIM) zero padding
            var pad = PAYLOAD_LIMIT - payloadLen
            while (pad > 0) {
                val n = min(pad, ZERO_PAD_CHUNK.capacity())
                val z = ZERO_PAD_CHUNK.duplicate().apply {
                    position(0); limit(n)
                }
                blk.put(z)
                pad -= n
            }

            // [end-4..end) CRC32C over [0 .. 4+payloadLen)
            blk.putInt(crc.value.toInt())

            blk.flip()
            onBlockReady(blk) // ownership transfer

        } catch (t: Throwable) {
            pool.release(blk)
            throw t
        } finally {
            scratch.clear()
        }
    }

    companion object {
        /**
         * Shared zero buffer for padding. Direct/LE to match the rest.
         * Read-only & duplicated per use to avoid mutations.
         */
        private val ZERO_PAD_CHUNK: ByteBuffer =
            ByteBuffer.allocateDirect(8192)  // 少し大きめ塊で帯域効率↑
                .order(ByteOrder.LITTLE_ENDIAN)
                .apply { clear() }
                .asReadOnlyBuffer()
    }
}