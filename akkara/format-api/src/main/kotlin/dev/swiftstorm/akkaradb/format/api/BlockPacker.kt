package dev.swiftstorm.akkaradb.format.api

import java.nio.ByteBuffer

/**
 * Packs a contiguous list of record bytes into fixed-size blocks
 * (32 KiB for the default “akk” format).
 */
interface BlockPacker {

    /** Size of a single block in bytes. */
    val blockSize: Int

    fun addRecord(record: ByteBuffer)

    fun flush()
}
