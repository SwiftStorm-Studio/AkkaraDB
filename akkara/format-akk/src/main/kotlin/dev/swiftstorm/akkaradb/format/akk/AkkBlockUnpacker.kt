package dev.swiftstorm.akkaradb.format.akk

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.BlockConst.PAYLOAD_LIMIT
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.vh.LE
import dev.swiftstorm.akkaradb.format.api.BlockUnpacker
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * High‑throughput 32KiB block unpacker (LE) — API‑preserving rewrite.
 *
 * Responsibilities
 *  • Validate the frame header `[payloadLen:u32 LE]` and bounds.
 *  • Iterate the payload as `[u32 recLen][bytes…]` and expose each record to caller.
 *  • CRC32C validation is *not* performed here (StripeReader already verified/repaired).
 *
 * Zero‑copy by default
 *  • Returned records are *views* (read‑only ByteBufferL wrapping a sub‑range of the block).
 *  • Callers MUST NOT release() these views to a pool; only the original 32KiB block is pool‑managed.
 *
 * Failure model
 *  • Malformed frames (negative/oversized lengths, overrun) → IllegalArgumentException.
 */
class AkkBlockUnpacker : BlockUnpacker, Closeable {

    override fun unpackInto(block: ByteBufferL, out: MutableList<ByteBufferL>) {
        out.clear()

        // We work on the raw ByteBuffer for absolute LE ops
        val bb: ByteBuffer = block.duplicate() // independent pos/lim, LE order applied by ByteBufferL
        require(bb.capacity() >= BLOCK_SIZE) { "block must be a 32KiB buffer" }

        // --- Frame header ---
        val payloadLen = LE.getInt(bb, 0)
        require(payloadLen in 0..PAYLOAD_LIMIT) {
            "invalid payloadLen=$payloadLen (limit=$PAYLOAD_LIMIT)"
        }
        val payloadStart = 4
        val payloadEndExcl = payloadStart + payloadLen
        require(payloadEndExcl <= BLOCK_SIZE - 4) {
            "payload overruns block: end=$payloadEndExcl > ${BLOCK_SIZE - 4}"
        }

        // --- Hot loop over [len | bytes] ---
        var p = payloadStart
        while (p < payloadEndExcl) {
            val recLen = LE.getInt(bb, p)
            require(recLen >= 0) { "negative recLen at offset=$p" }
            val recStart = p + 4
            val recEndExcl = recStart + recLen
            require(recEndExcl <= payloadEndExcl) {
                "record overruns payload: end=$recEndExcl > $payloadEndExcl"
            }

            // Zero‑copy read‑only view of [recStart, recEndExcl)
            val view: ByteBuffer = bb.duplicate().position(recStart).limit(recEndExcl)
                .asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN)
            out += ByteBufferL.wrap(view)

            p = recEndExcl
        }
    }

    override fun close() { /* stateless */
    }
}
