package dev.swiftstorm.akkaradb.format.api

import java.io.Closeable
import java.nio.ByteBuffer

interface StripeReader : Closeable {
    fun readStripe(): List<ByteBuffer>?
}