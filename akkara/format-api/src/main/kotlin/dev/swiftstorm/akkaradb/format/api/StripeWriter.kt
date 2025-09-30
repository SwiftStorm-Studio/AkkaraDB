/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 * LGPL-3.0-or-later
 */
package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.io.Closeable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

/**
 * Stripe-aligned block appender.
 *
 * - **Append-only**; never overwrites or reads.
 * - **Block size is fixed** (e.g., 32 KiB). Exactly k data blocks form a stripe;
 *   then m parity blocks are derived and written at the **same lane offset**.
 * - **Durability model**:
 *   - SYNC flush: returns when data+parity are forced (fdatasync/force(false)).
 *   - ASYNC flush (FastMode): returns a ticket whose future completes once durability is reached.
 *
 * Concurrency: unless stated otherwise by the implementation, calls are **single-producer**.
 */
interface StripeWriter : Closeable {

    /** Number of data lanes. */
    val k: Int

    /** Number of parity lanes (0 = no redundancy). */
    val m: Int

    /** Fixed block size in bytes (e.g., 32 * 1024). */
    val blockSize: Int

    /** Returns true if the writer is configured for FastMode (async fsync). */
    val isFastMode: Boolean get() = false

    /** Number of blocks currently accumulated for the open (not-yet-sealed) stripe. */
    val pendingInStripe: Int

    /** Last fully **sealed** (written+fsync issued) stripe index (0-based). */
    val lastSealedStripe: Long

    /** Last fully **durable** stripe index (fsync completed). */
    val lastDurableStripe: Long

    /** Current stripe index if the next block starts a new stripe = lastSealedStripe + (pendingInStripe==0 ? 0 : 1). */
    val currentStripeIndex: Long
        get() = lastSealedStripe + if (pendingInStripe == 0) 0 else 1

    // --------------------------------------------------------------------
    // Write path
    // --------------------------------------------------------------------

    /**
     * Appends one **data** [block] into the in-flight stripe buffer.
     * When [k] data blocks are accumulated, the implementation:
     *  - computes parity (if m>0),
     *  - issues lane writes (data + parity) in a minimal syscall pattern,
     *  - enrolls the stripe into group-commit scheduling.
     *
     * @throws IllegalStateException if block size != [blockSize]
     * @throws IllegalStateException if more than [k] data blocks are added for the same stripe
     */
    fun addBlock(block: ByteBufferL)

    /**
     * Flush policy hints for group-commit.
     *
     * The writer may flush earlier/later, but should **strive** to respect these bounds.
     */
    var flushPolicy: FlushPolicy

    /**
     * Forces buffered stripes according to [mode] and returns a [CommitTicket].
     *
     * - SYNC: blocks until durability; ticket.future is already completed.
     * - ASYNC: schedules fsync on a background executor and completes future on durability.
     *
     * The returned ticket’s [uptoStripe] is the **highest sealed stripe index** covered by this flush call.
     * Durability for SYNC is immediate; for ASYNC it is signalled by the future.
     */
    fun flush(mode: FlushMode = FlushMode.SYNC): CommitTicket

    /**
     * Finishes the current partial stripe if any — by **sealing** it only when it has exactly [k] data blocks.
     * No implicit padding is performed; callers must provide complete stripes.
     * Useful before close or mode switches.
     */
    fun sealIfComplete(): Boolean

    /**
     * Restores internal counters and file offsets after reopening existing lanes.
     * If the implementation supports it, [stripeIndex] should be the index **after** the last durable stripe.
     * Default is conservative and rejects non-zero seeks.
     */
    fun seek(stripeIndex: Long) {
        require(stripeIndex == 0L) { "seek() not supported by this implementation" }
    }

    /**
     * Scans lane tails, validates CRC/length, and positions the writer at the first **appendable** stripe index.
     * Implementations may do a lightweight footer/manifest-based fast path.
     */
    fun recover(): RecoveryResult = RecoveryResult(0, 0, false)

    /**
     * Snapshot of internal performance counters.
     */
    fun metrics(): StripeMetricsSnapshot = StripeMetricsSnapshot()
}

/* ============================ Value types ============================ */

/** Flush mode for durability. */
enum class FlushMode { SYNC, ASYNC }

/**
 * Group-commit policy.
 * - [maxBlocks] : flush when this many blocks (across stripes) are pending for fsync.
 * - [maxMicros] : or when this many microseconds elapsed since the last fsync group.
 * - [executor]  : service for ASYNC fsync; ignored for SYNC.
 */
data class FlushPolicy(
    val maxBlocks: Int = 32,
    val maxMicros: Long = 500, // µs
    val executor: Executor? = null
)

/**
 * Result of a flush() call.
 * - [uptoStripe] : highest sealed stripe index included in this commit group.
 * - [future]     : completes with the highest **durable** stripe index when fsync finishes.
 *                  For SYNC flushes, this is already completed.
 */
data class CommitTicket(
    val uptoStripe: Long,
    val future: CompletableFuture<Long>
)

/**
 * Recovery outcome.
 * - [lastSealed]   : last stripe that is fully formed and written.
 * - [lastDurable]  : last stripe that is provably durable (via manifest/superblock/etc).
 * - [truncatedTail]: true if incomplete/garbled tail blocks were discarded.
 */
data class RecoveryResult(
    val lastSealed: Long,
    val lastDurable: Long,
    val truncatedTail: Boolean
)

/**
 * One-shot metrics snapshot. All counters are monotonic since writer creation.
 */
data class StripeMetricsSnapshot(
    val sealedStripes: Long = 0,
    val durableStripes: Long = 0,
    val bytesWrittenData: Long = 0,
    val bytesWrittenParity: Long = 0,
    val parityMicros: Long = 0,
    val laneWriteMicros: Long = 0,
    val fsyncMicros: Long = 0,
    val maxBackpressureMicros: Long = 0
)
