/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
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
 * - Validating and optionally reconstructing missing lanes using a [ParityCoder].
 * - Exposing decoded payload slices and retaining ownership of the
 *   underlying lane buffers until released.
 */
interface StripeReader : Closeable {

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
        /**
         * Releases all lane blocks back to the associated [BufferPool].
         * Must be invoked exactly once to avoid memory leaks.
         */
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
}
