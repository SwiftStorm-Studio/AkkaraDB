package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL

/**
 * Error-correcting extension for ParityCoder (supports unknown-location errors).
 *
 * - Works on the same fixed block size as [ParityCoder.blockSize].
 * - Must satisfy: 2*t + s ≤ m  (t = unknown-position errors, s = known erasures, m = parityCount).
 * - Inputs use the same nullable-array convention as ParityCoder.reconstruct().
 */
interface ECParityCoder : ParityCoder {

    /**
     * Allocate-and-return convenience: recovers all missing **data** blocks.
     * Implementations may allocate output buffers internally (e.g., from a pool).
     *
     * @param presentData   size k; null where data is missing or suspected bad
     * @param presentParity size m; null where parity is missing
     * @param knownErasures optional indices (0..k-1) of lanes known missing
     * @return recovered indices and buffers (indices are data-lane indices)
     * @throws IllegalArgumentException on violated bounds (2*t + s > m, n>255, size mismatch)
     */
    fun decodeEC(
        presentData: Array<ByteBufferL?>,
        presentParity: Array<ByteBufferL?>,
        knownErasures: IntArray? = null
    ): Pair<IntArray, Array<ByteBufferL>>

    /**
     * Zero-allocation variant: caller supplies output slots.
     *
     * @param presentData   size k; null where data is missing or suspected bad
     * @param presentParity size m; null where parity is missing
     * @param knownErasures optional indices (0..k-1) of lanes known missing
     * @param outIndices    preallocated array to receive recovered data indices (length ≥ max recoverable)
     * @param outBuffers    preallocated buffers (length ≥ outIndices.size); each buffer must have ≥ blockSize remaining
     * @return number of recovered blocks written into [outIndices]/[outBuffers]
     */
    fun decodeECInto(
        presentData: Array<ByteBufferL?>,
        presentParity: Array<ByteBufferL?>,
        knownErasures: IntArray?,
        outIndices: IntArray,
        outBuffers: Array<ByteBufferL>
    ): Int
}
