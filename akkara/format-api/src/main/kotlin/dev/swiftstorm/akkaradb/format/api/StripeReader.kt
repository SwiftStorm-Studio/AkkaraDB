package dev.swiftstorm.akkaradb.format.api

import java.io.Closeable
import java.nio.ByteBuffer

interface StripeReader : Closeable {
    /** A stripe with both payloads and lane blocks. */
    data class Stripe(
        val payloads: List<ByteBuffer>,
        val laneBlocks: List<ByteBuffer>,
        private val pool: dev.swiftstorm.akkaradb.common.BufferPool
    ) : AutoCloseable {
        override fun close() {
            laneBlocks.forEach(pool::release)
        }
    }

    fun readStripe(): Stripe?
}