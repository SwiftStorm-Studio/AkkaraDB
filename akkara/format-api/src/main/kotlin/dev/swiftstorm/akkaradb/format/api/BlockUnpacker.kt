package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL

/**
 * Parses a fixed-size block (32 KiB) and validates the CRC32.
 * Returns a view (`ByteBuffer.slice()`) of the payload region.
 *
 * Layout  : `[4B len][payload][padding][4B CRC]`
 *
 * Position: block.position() must be 0.
 */
interface BlockUnpacker {

    /** @throws dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException if CRC mismatch or len invalid. */
    fun unpack(block: ByteBufferL): List<ByteBufferL>
}