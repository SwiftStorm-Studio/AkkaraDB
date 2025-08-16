package dev.swiftstorm.akkaradb.format.api

import java.io.Closeable
import java.nio.ByteBuffer

interface StripeReader : Closeable {
    data class Stripe(
        val payloads: List<ByteBuffer>,
        val laneBlocks: List<ByteBuffer>
    )

    fun readStripe(): Stripe?
}