/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * See <https://www.gnu.org/licenses/>.
 */

package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.io.Closeable

/**
 * Reader abstraction for sequentially consuming *stripes* from a segment.
 *
 * A stripe is the unit of redundancy in AkkaraDB's append-only format:
 * it consists of multiple fixed-size lane blocks (`k` data lanes and
 * optionally `m` parity lanes). Each stripe may contain one or more
 * encoded records.
 *
 * Implementations are responsible for:
 * - Reading the full set of lane blocks that make up a stripe.
 * - Validating and optionally reconstructing missing lanes using a ParityCoder.
 * - Exposing decoded payload slices and retaining ownership of the
 *   underlying lane buffers until released.
 *
 * Concurrency: unless stated otherwise by the implementation, calls are single-consumer.
 */
interface StripeReader : Closeable {

    /** Number of data lanes. */
    val k: Int

    /** Number of parity lanes (0 = no redundancy). */
    val m: Int

    /** Fixed block size in bytes (e.g., 32 * 1024). */
    val blockSize: Int

    /** Next stripe index (0-based) that will be returned by [readStripe]. */
    val nextStripeIndex: Long

    /**
     * Represents a decoded stripe with both payload slices and
     * the underlying lane blocks.
     *
     * The caller **must** call [close] when finished to release
     * the backing buffers back to the [BufferPool].
     *
     * @property payloads decoded payload slices (record data views).
     * @property laneBlocks the raw lane blocks (data and parity), each
     *         a pooled [ByteBufferL] of fixed size.
     * @property pool the buffer pool from which blocks were obtained.
     */
    data class Stripe(
        val payloads: List<ByteBufferL>,
        val laneBlocks: List<ByteBufferL>,
        private val pool: BufferPool
    ) : AutoCloseable {
        /** Releases all lane blocks back to the associated [BufferPool]. */
        override fun close() {
            laneBlocks.forEach(pool::release)
        }
    }

    /**
     * Reads the next stripe from the underlying source.
     *
     * @return a [Stripe] containing payloads and backing blocks,
     *         or `null` if end-of-stream is reached.
     * @throws dev.swiftstorm.akkaradb.format.exception.CorruptedBlockException
     *         if the stripe cannot be decoded due to corruption.
     */
    fun readStripe(): Stripe?

    /**
     * Positions the reader to the given [stripeIndex] (0-based).
     * Implementations should treat invalid or non-existing positions as errors.
     */
    fun seek(stripeIndex: Long)

    /**
     * Scans lane tails, validates CRC/length as available, and positions the reader
     * at the first **readable** stripe index after recovery.
     *
     * Returns the same structure as Writer の [StripeWriter.recover] と対応づけるため、
     * [ReaderRecoveryResult] を返す（フィールドは Writer 側の [RecoveryResult] に合わせている）。
     */
    fun recover(): ReaderRecoveryResult = ReaderRecoveryResult(0, 0, false)

    /**
     * Snapshot of internal performance counters (対になる Writer の metrics と同様にワンショット)。
     */
    fun metrics(): ReaderMetricsSnapshot = ReaderMetricsSnapshot()
}

/**
 * Recovery outcome for the reader.
 * - [lastSealed]   : last stripe that is fully formed and readable.
 * - [lastDurable]  : last stripe that is provably durable (manifest/superblock等を持つ場合)。
 *                    不明な場合は [lastSealed] と同じ値を返してよい。
 * - [truncatedTail]: true if incomplete/garbled tail blocks were discarded.
 */
data class ReaderRecoveryResult(
    val lastSealed: Long,
    val lastDurable: Long,
    val truncatedTail: Boolean
)

/**
 * One-shot metrics snapshot for the reader.
 * Counter semantics mirror Writer の [StripeMetricsSnapshot] に対応。
 */
data class ReaderMetricsSnapshot(
    val stripesReturned: Long = 0,
    val bytesReadData: Long = 0,
    val bytesReadParity: Long = 0,
    val readMicros: Long = 0,
    val verifyMicros: Long = 0,
    val reconstructMicros: Long = 0
)
