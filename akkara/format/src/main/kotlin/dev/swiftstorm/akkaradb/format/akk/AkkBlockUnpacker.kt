package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.format.BlockUnpacker
import dev.swiftstorm.akkaradb.format.akk.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * Unpacks a 32-KiB “akk” data block and returns the payload slice.
 *
 * Block layout:  [4B length][payload][zero-padding][4B CRC32]
 *
 * * The caller must pass a buffer whose `position == 0`
 *   and whose `remaining() == 32 KiB`.
 * * The returned buffer is **read-only** and shares the same
 *   backing memory (zero-copy).
 */
class AkkBlockUnpacker : BlockUnpacker {

    private val crc = CRC32()

    override fun unpack(block: ByteBuffer): ByteBuffer {
        require(block.remaining() == BLOCK_SIZE) {
            "Block size ${block.remaining()} B ≠ $BLOCK_SIZE B"
        }

        /* ---- 1. read length ---- */
        val len = block.int          // advances position from 0 → 4
        if (len < 0 || len > PAYLOAD_LIMIT) {
            throw CorruptedBlockException("Invalid payload length: $len")
        }

        /* ---- 2. read stored CRC32 (last 4 bytes) ---- */
        val crcStored = run {
            val savedPos = block.position()
            val crcPos   = BLOCK_SIZE - 4
            val crcVal   = block.getInt(crcPos)
            block.position(savedPos)      // restore
            crcVal
        }

        /* ---- 3. compute CRC32 over [length + payload] ---- */
        crc.reset()
        val dup = block.duplicate()
        dup.limit(4 + len)                // includes length field
        dup.position(0)
        crc.update(dup)
        val crcCalc = crc.value.toInt()

        if (crcCalc != crcStored) {
            throw CorruptedBlockException(
                "CRC mismatch: stored=0x${crcStored.toUInt().toString(16)} " +
                        "calc=0x${crcCalc.toUInt().toString(16)}"
            )
        }

        /* ---- 4. slice the payload ---- */
        val slice = run {
            val payload = block.duplicate()
            payload.position(4)           // start of payload
            payload.limit(4 + len)
            payload.slice().asReadOnlyBuffer()
        }
        return slice
    }

    /* ---- constants ---- */
    private companion object {
        const val PAYLOAD_LIMIT = BLOCK_SIZE - 8   // 4B len + 4B CRC
    }
}
