package dev.swiftstorm.akkaradb.format.api

import java.nio.ByteBuffer

/**
 * Packs a contiguous list of record bytes into fixed-size blocks
 * (32 KiB for the default “akk” format).
 */
interface BlockPacker {

    /** Size of a single block in bytes. */
    val blockSize: Int

    /**
     * Appends [record] to the internal buffer. When the buffer reaches
     * [blockSize], the provided [consumer] is invoked with a read-only
     * slice representing one complete block.
     *
     * Implementations MUST reuse a scratch buffer internally to avoid
     * per-block allocations.
     */
    fun addRecord(record: ByteBuffer, consumer: (ByteBuffer) -> Unit)

    /**
     * Flushes any partial block (with zero-padding) to [consumer].
     * Must be called before closing the writer.
     */
    fun flush(consumer: (ByteBuffer) -> Unit)
}
