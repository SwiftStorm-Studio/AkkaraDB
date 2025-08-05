package dev.swiftstorm.akkaradb.engine.util

import java.io.OutputStream
import java.nio.ByteBuffer

class ByteBufferOutputStream(val buf: ByteBuffer) : OutputStream() {
    override fun write(b: Int) {
        buf.put(b.toByte())
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        buf.put(b, off, len)
    }
}