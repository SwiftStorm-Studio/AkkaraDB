package dev.swiftstorm.akkaradb.format.api

import java.nio.ByteBuffer

/**
 * Writes fixed-size blocks into one or more lane files
 * (data_*.akd, parity_*.akp). A stripe is defined as a group of
 * [k] data blocks plus [m] parity blocks written at the same
 * logical offset across all lanes.
 *
 * Implementations are *append-only*; they never overwrite or read.
 */
interface StripeWriter : AutoCloseable {

    /** Number of data lanes in this stripe. */
    val k: Int

    /** Number of parity lanes (0 = no redundancy). */
    val m: Int

    /** Current stripe index (0-based). */
    val stripesWritten: Long

    /**
     * Appends exactly one data [block] (e.g. 32 KiB from BlockPacker)
     * to the next position in lane 0. Implementations may buffer the
     * block until [k] blocks are available, then write the full stripe
     * in one I/O batch.
     *
     * @throws IllegalStateException if more than [k] blocks are added
     *         to the current stripe.
     */
    fun addBlock(block: ByteBuffer)

    /**
     * Forces buffered blocks to disk (data and parity) and returns
     * the stripe index that has become durable.
     *
     * The call is blocking; durability is guaranteed when it returns.
     */
    fun flush(): Long

    /**
     * Restores the internal counter after reopening existing lane files.
     * Must be called *before* the first addBlock()/flush().
     *
     * Default impl allows engines to call it safely even on writers
     * that don’t care (seek(0) is a no-op).
     */
    fun seek(stripeIndex: Long) {
        require(stripeIndex == 0L) { "seek() not supported by this implementation" }
    }

    /** Closes all lane files and releases resources. */
    override fun close()
}
