package dev.swiftstorm.akkaradb.common

object BlockConst {
    const val BLOCK_SIZE = 32 * 1024            // 32 KiB
    const val PAYLOAD_LIMIT = BLOCK_SIZE - 8      // length + CRC
    const val MAX_RECORD = PAYLOAD_LIMIT       // Single record upper bound
}