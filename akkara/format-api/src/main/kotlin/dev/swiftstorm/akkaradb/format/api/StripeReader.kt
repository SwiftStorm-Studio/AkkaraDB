package dev.swiftstorm.akkaradb.format.api

import dev.swiftstorm.akkaradb.common.BufferPool
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.io.Closeable

interface StripeReader : Closeable {
    /** A stripe with both payloads and lane blocks. */
    data class Stripe(
        val payloads: List<ByteBufferL>,
        val laneBlocks: List<ByteBufferL>,
        private val pool: BufferPool
    ) : AutoCloseable {
        override fun close() {
            laneBlocks.forEach(pool::release)
        }
    }

    fun readStripe(): Stripe?
}