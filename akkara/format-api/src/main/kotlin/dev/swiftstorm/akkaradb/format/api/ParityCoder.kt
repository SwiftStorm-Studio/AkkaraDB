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

import dev.swiftstorm.akkaradb.common.BlockConst.BLOCK_SIZE
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.io.Closeable

/**
 * ParityCoder — zero-allocation parity codec for AkkaraDB stripes (ByteBufferL-only).
 *
 * Design goals:
 *  - **ByteBufferL everywhere** (LE enforced, direct-friendly).
 *  - **No hidden allocations** on the hot path; caller provides all buffers.
 *  - **Fixed block size** contract (default 32 KiB).
 *  - **General erasure** support up to [parityCount] losses (RS for m>=3).
 *
 * Thread-safety:
 *  - Implementations are typically stateless; per-thread scratch can be attached via [attachScratch].
 */
interface ParityCoder : Closeable {

    /** Number of parity lanes `m` (0 = none). */
    val parityCount: Int

    /** Fixed block size (bytes) required by this coder. */
    val blockSize: Int get() = BLOCK_SIZE

    /** Type hint for metrics/logging. */
    val kind: ParityKind

    /** True if this coder can correct arbitrary erasures up to [parityCount] (e.g., RS/RS_EC). */
    val supportsErrorCorrection: Boolean get() = parityCount >= 1

    /**
     * Encode parity **in place** into [parityOut] from [data].
     *
     * Contract:
     *  - `data.size == k`, `parityOut.size == m`.
     *  - Each buffer must expose **at least [blockSize] bytes remaining** (position respected).
     *  - `parityOut` contents on entry are ignored and fully overwritten.
     *
     * No allocations; no temporary arrays/views unless the implementation documented otherwise.
     */
    fun encodeInto(
        data: Array<ByteBufferL>,
        parityOut: Array<ByteBufferL>
    )

    /**
     * Verify that [parity] matches [data] (recompute and compare).
     * Implementations should short-circuit on first mismatch.
     */
    fun verify(
        data: Array<ByteBufferL>,
        parity: Array<ByteBufferL>
    ): Boolean

    /**
     * Reconstruct lost blocks (data and/or parity) up to [parityCount] erasures.
     *
     * @param lostDataIdx   indices (0..k-1) of lost **data** blocks
     * @param lostParityIdx indices (0..m-1) of lost **parity** blocks
     * @param data          array of size k; present entries non-null; lost entries null
     * @param parity        array of size m; present entries non-null; lost entries null
     * @param outData       destination buffers for lost data (size == lostDataIdx.size)
     * @param outParity     destination buffers for lost parity (size == lostParityIdx.size)
     * @return total reconstructed blocks
     *
     * All destination buffers must have ≥ [blockSize] remaining.
     * @throws IllegalArgumentException if erasures exceed [parityCount] or sizes/positions are inconsistent.
     */
    fun reconstruct(
        lostDataIdx: IntArray,
        lostParityIdx: IntArray,
        data: Array<ByteBufferL?>,
        parity: Array<ByteBufferL?>,
        outData: Array<ByteBufferL>,
        outParity: Array<ByteBufferL>
    ): Int

    /**
     * Optional per-thread scratch attachment for heavy coders (e.g., RS).
     * Engines can pass a pooled direct buffer (as ByteBufferL) to avoid TLAs.
     */
    fun attachScratch(buf: ByteBufferL?) { /* default: no-op */
    }

    /** Conservative upper bound for desirable scratch bytes per thread (for engine-side pooling). */
    fun scratchBytesHint(k: Int = -1, m: Int = -1): Int = 0

    override fun close() { /* default: stateless no-op */
    }
}

/** Kind hint for metrics/logging. */
enum class ParityKind { NONE, XOR, DUAL_XOR, RS, RS_EC }
