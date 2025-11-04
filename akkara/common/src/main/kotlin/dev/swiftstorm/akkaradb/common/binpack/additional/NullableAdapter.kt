package dev.swiftstorm.akkaradb.common.binpack.additional

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

/**
 * Adapter for nullable values with a 1-byte presence flag.
 * Encoding: [i8 flag][value?]
 *   - flag = 0 -> null
 *   - flag = 1 -> present, then encoded by inner
 */
class NullableAdapter<T : Any>(
    private val inner: TypeAdapter<T>
) : TypeAdapter<T?> {

    override fun estimateSize(value: T?): Int {
        return if (value == null) 1 else 1 + inner.estimateSize(value)
    }

    override fun write(value: T?, buffer: ByteBufferL) {
        if (value == null) {
            buffer.i8 = 0
        } else {
            buffer.i8 = 1
            inner.write(value, buffer)
        }
    }

    override fun read(buffer: ByteBufferL): T? {
        val flag = buffer.i8 and 1
        return if (flag != 0) inner.read(buffer) else null
    }
}