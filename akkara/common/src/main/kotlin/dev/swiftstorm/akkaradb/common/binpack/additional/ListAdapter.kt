package dev.swiftstorm.akkaradb.common.binpack.additional

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterSetting
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

/**
 * Adapter for List<T> with prefixed size.
 * Encoding: [i32 size][`element...`]
 */
class ListAdapter<T>(
    private val elementAdapter: TypeAdapter<T>
) : TypeAdapter<List<T>> {

    override fun estimateSize(value: List<T>): Int {
        var total = 4 // size header
        for (e in value) total += elementAdapter.estimateSize(e)
        return total
    }

    override fun write(value: List<T>, buffer: ByteBufferL) {
        buffer.i32 = value.size
        for (e in value) elementAdapter.write(e, buffer)
    }

    override fun read(buffer: ByteBufferL): List<T> {
        val size = buffer.i32
        require(size in 0..AdapterSetting.maxCollectionSize) {
            "Collection size $size exceeds configured limit (${AdapterSetting.maxCollectionSize})"
        }
        val list = ArrayList<T>(size)
        repeat(size) { list.add(elementAdapter.read(buffer)) }
        return list
    }
}