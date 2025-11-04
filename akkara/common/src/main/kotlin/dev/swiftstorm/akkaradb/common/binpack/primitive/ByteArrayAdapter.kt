package dev.swiftstorm.akkaradb.common.binpack.primitive

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

object ByteArrayAdapter : TypeAdapter<ByteArray> {
    override fun estimateSize(value: ByteArray) = 4 + value.size

    override fun write(value: ByteArray, buffer: ByteBufferL) {
        buffer.i32 = value.size
        if (value.isNotEmpty()) buffer.putBytes(value)
    }

    override fun read(buffer: ByteBufferL): ByteArray {
        val size = buffer.i32
        val out = ByteArray(size)
        var i = 0
        while (i < size) {
            out[i] = buffer.i8.toByte()
            i++
        }
        return out
    }
}