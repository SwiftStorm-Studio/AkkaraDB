package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.format.BlockPacker
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * Packs variable-size records into fixed 32 KiB blocks.
 *
 * Block layout
 * ```
 * [4B payloadLen][payload...][zero-padding][4B CRC32] = 32 KiB
 * ```
 *
 * * Works with **Direct** or **Heap** `ByteBuffer`s.
 * * Caller supplies a `consumer` that receives a read-only buffer
 *   whenever a full block is ready.
 */
class AkkBlockPackerDirect : BlockPacker {

    override val blockSize: Int = BLOCK_SIZE

    /** Scratch buffer for accumulating TLV records. */
    private val scratch: ByteBuffer = ByteBuffer.allocateDirect(blockSize)
    private val crc = CRC32()

    /* ---------- public API ---------- */

    override fun addRecord(record: ByteBuffer, consumer: (ByteBuffer) -> Unit) {
        val len = record.remaining()
        require(len <= MAX_RECORD) {
            "Single record ($len B) exceeds block payload limit ($MAX_RECORD B)"
        }

        if (scratch.position() + len > PAYLOAD_LIMIT) {
            emitBlock(consumer)               // flush full block first
        }
        scratch.put(record.duplicate())       // copy bytes into scratch
    }

    override fun flush(consumer: (ByteBuffer) -> Unit) {
        if (scratch.position() > 0) {
            emitBlock(consumer)
        }
    }

    /* ---------- internal ---------- */

    private fun emitBlock(consumer: (ByteBuffer) -> Unit) {
        val payloadLen = scratch.position()
        scratch.flip()

        val block = ByteBuffer.allocateDirect(blockSize)
        block.putInt(payloadLen)
        block.put(scratch)

        while (block.position() < PAYLOAD_LIMIT + 4) block.put(0)  // padding

        /* ---- CRC32 without heap copy ---- */
        crc.reset()
        val dup = block.duplicate()          // preserve original positions
        dup.limit(4 + payloadLen)            // [length + payload] only
        dup.position(0)
        crc.update(dup)                      // JDK 9+ supports this

        block.position(PAYLOAD_LIMIT + 4)
        block.putInt(crc.value.toInt())

        block.flip()
        consumer(block.asReadOnlyBuffer())
        scratch.clear()
    }


    /* ---------- constants ---------- */

    private companion object {
        const val BLOCK_SIZE   = 32 * 1024            // 32 KiB
        const val PAYLOAD_LIMIT = BLOCK_SIZE - 8      // length + CRC
        const val MAX_RECORD    = PAYLOAD_LIMIT
    }
}
