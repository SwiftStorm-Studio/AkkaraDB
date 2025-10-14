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
import kotlin.math.min

/**
 * 32KiB fixed-size block packer (LE). Keeps the existing BlockPacker interface
 * and introduces `fastMode` for more aggressive zero-padding.
 *
 * Block layout:
 * [0..3]     : payloadLen (u32 LE)
 * [4..4+N)   : payload (varlen sequence of [u32 recLen][bytes])
 * [4+N..-5]  : zero padding
 * [-4..-1]   : CRC32C (stamped later by the StripeWriter)
 *
 * Responsibility split:
 * - This class ONLY builds the framed payload and zero pads up to (BLOCK_SIZE-4).
 * - CRC is stamped by `AkkStripeWriter.sealStripe()` over the range [0..BLOCK_SIZE-4).
 *
 * Fast/Durable design note:
 * - `fastMode=false` uses a smaller zero buffer (8KiB) and simple loops.
 * - `fastMode=true` uses a larger zero buffer (64KiB) to reduce put() calls and stabilize P99.
 *
 * Ownership:
 * - `onBlockReady` takes ownership of the emitted 32KiB direct buffer.
 * - Caller-provided `record` buffers are never mutated (we duplicate before reading).
 */
class AkkBlockPacker(
    private val onBlockReady: (ByteBufferL) -> Unit,
    private val pool: BufferPool = Pools.io(),
    private val fastMode: Boolean = false
) : BlockPacker, Closeable {

    // Currently building 32KiB block
    private var scratch: ByteBufferL = pool.get(BLOCK_SIZE)

    // Payload write position (first 4 bytes are payloadLen)
    private var writePos: Int = 4
    private var closed = false

    override fun addRecord(record: ByteBufferL) {
        check(!closed) { "packer already closed" }
        val recBB: ByteBuffer = record.duplicate() // do not touch caller's position/limit
        val recLen = recBB.remaining()
        val need = 4 + recLen // [u32 len] + body
        require(need <= PAYLOAD_LIMIT) {
            "Encoded record (${need}B) exceeds payload limit ($PAYLOAD_LIMIT)"
        }
        // If the payload area would overflow, emit current block first
        if (writePos + need > PAYLOAD_LIMIT) emitBlock()

        // ==== write ====
        val bb = scratch.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        // tentatively update total payload length at head
        bb.putInt(writePos - 4, (writePos - 4) + need)
        bb.putInt(writePos, recLen)
        bb.position(writePos + 4)
        bb.limit(writePos + 4 + recLen)
        bb.put(recBB.duplicate())
        writePos += need
    }

    override fun flush() {
        check(!closed) { "packer already closed" }
        if (writePos > 4) emitBlock()
    }

    override fun close() {
        if (closed) return
        try {
            flush()
        } finally {
            closed = true
        }
    }

    // ---- internals ----
    private fun emitBlock() {
        val bb = scratch.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        val payloadLen = writePos - 4
        // seal payloadLen at head
        bb.putInt(0, payloadLen)

        // zero pad [writePos .. BLOCK_SIZE-4)
        zeroPad(bb, writePos, BLOCK_SIZE - 4)

        // hand off ownership and allocate a fresh block
        val full = scratch
        scratch = pool.get(BLOCK_SIZE)
        writePos = 4
        onBlockReady(full)
    }

    private fun zeroPad(bb: ByteBuffer, from: Int, toExcl: Int) {
        val need = toExcl - from
        if (need <= 0) return
        val z = if (fastMode) ZERO_PAD_CHUNK_64K else ZERO_PAD_CHUNK
        var pos = from
        while (pos < toExcl) {
            val n = min(z.limit(), toExcl - pos)
            val dst = bb.duplicate().position(pos).limit(pos + n)
            val src = z.duplicate().position(0).limit(n)
            dst.put(src)
            pos += n
        }
    }

    companion object {
        /** 8KiB zero buffer (read-only, direct, LE) */
        private val ZERO_PAD_CHUNK: ByteBuffer =
            ByteBuffer.allocateDirect(8 * 1024)
                .order(ByteOrder.LITTLE_ENDIAN)
                .apply { clear() }
                .asReadOnlyBuffer()

        /** 64KiB zero buffer for fastMode to reduce put() calls */
        private val ZERO_PAD_CHUNK_64K: ByteBuffer =
            ByteBuffer.allocateDirect(64 * 1024)
                .order(ByteOrder.LITTLE_ENDIAN)
                .apply { clear() }
                .asReadOnlyBuffer()
    }
}
