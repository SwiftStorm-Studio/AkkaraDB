package dev.swiftstorm.akkaradb.format.akk

object BlockConst {
    const val BLOCK_SIZE = 32 * 1024
    private const val HEADER = 4
    private const val FOOTER = 4
    const val MAX_PAYLOAD = BLOCK_SIZE - HEADER - FOOTER
}