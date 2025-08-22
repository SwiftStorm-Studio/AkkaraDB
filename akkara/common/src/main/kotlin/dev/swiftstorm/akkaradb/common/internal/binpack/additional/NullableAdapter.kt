package dev.swiftstorm.akkaradb.common.internal.binpack.additional

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

class NullableAdapter<T : Any>(
    private val inner: TypeAdapter<T>
) : TypeAdapter<T?> {
    override fun estimateSize(value: T?): Int {
        return if (value == null) 1 else 1 + inner.estimateSize(value)
    }

    override fun write(value: T?, buffer: ByteBuffer) {
        if (value == null) {
            buffer.put(0)
        } else {
            buffer.put(1)
            inner.write(value, buffer)
        }
    }

    override fun read(buffer: ByteBuffer): T? {
        return if ((buffer.get().toInt() and 1) != 0) inner.read(buffer) else null
    }
}
