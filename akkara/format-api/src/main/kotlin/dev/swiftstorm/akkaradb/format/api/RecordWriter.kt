package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record

/**
 * Serialises a [Record] into a binary representation understood by the
 * Block-/Stripe-layer. Implementations **must not** perform any I/O;
 * they only fill the supplied buffer or return a `ByteArray`.
 */
interface RecordWriter {

    /**
     * Encodes [record] into the provided [ByteBufferL].
     *
     * @param record The logical record to encode.
     * @param dest   Destination buffer positioned at the write offset.
     * @return Number of bytes written.
     * @throws java.nio.BufferOverflowException if the buffer is too small.
     */
    fun write(record: Record, dest: ByteBufferL): Int

    /**
     * Calculates an upper bound on the encoded size of [record].
     * Implementations should be fast (<50 ns) and side-effect free.
     */
    fun computeMaxSize(record: Record): Int
}
