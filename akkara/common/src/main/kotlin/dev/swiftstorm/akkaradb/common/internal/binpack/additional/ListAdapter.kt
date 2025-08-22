package dev.swiftstorm.akkaradb.common.internal.binpack.additional

import dev.swiftstorm.akkaradb.common.internal.binpack.AdapterSetting
import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

class ListAdapter<T>(
    private val elementAdapter: TypeAdapter<T>
) : TypeAdapter<List<T>> {

    override fun estimateSize(value: List<T>): Int {
        var total = 4
        for (e in value) total += elementAdapter.estimateSize(e)
        return total
    }

    override fun write(value: List<T>, buffer: ByteBuffer) {
        buffer.putInt(value.size)
        for (e in value) elementAdapter.write(e, buffer)
    }

    override fun read(buffer: ByteBuffer): List<T> {
        val size = buffer.int
        require(size in 0..AdapterSetting.maxCollectionSize) {
            "Collection size $size exceeds configured limit (${AdapterSetting.maxCollectionSize})"
        }
        val list = ArrayList<T>(size)
        repeat(size) { list.add(elementAdapter.read(buffer)) }
        return list
    }
}
