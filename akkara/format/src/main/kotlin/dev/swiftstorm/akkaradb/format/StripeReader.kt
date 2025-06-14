package dev.swiftstorm.akkaradb.format

import java.io.Closeable
import java.nio.ByteBuffer

interface StripeReader : Closeable {
    fun readStripe(): List<ByteBuffer>?
}