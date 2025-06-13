package dev.swiftstorm.akkaradb.format

import dev.swiftstorm.akkaradb.common.Record
import java.nio.ByteBuffer

/**
 * Serialises a [Record] into a binary representation understood by the
 * Block-/Stripe-layer. Implementations **must not** perform any I/O;
 * they only fill the supplied buffer or return a `ByteArray`.
 */
interface RecordWriter {

    /**
     * Encodes [record] into the provided [ByteBuffer].
     *
     * @param record The logical record to encode.
     * @param dest   Destination buffer positioned at the write offset.
     * @return Number of bytes written.
     * @throws java.nio.BufferOverflowException if the buffer is too small.
     */
    fun write(record: Record, dest: ByteBuffer): Int

    /**
     * Convenience helper that allocates a new byte array, writes the
     * record, and returns the encoded slice.
     */
    fun toByteArray(record: Record): ByteArray = ByteBuffer
        .allocate(computeMaxSize(record))
        .also { write(record, it) }
        .flip()
        .let { buf ->
            ByteArray(buf.remaining()).also { buf.get(it) }
        }

    /**
     * Calculates an upper bound on the encoded size of [record].
     * Implementations should be fast (<50 ns) and side-effect free.
     */
    fun computeMaxSize(record: Record): Int
}
