package dev.swiftstorm.akkaradb.common.internal.binpack.additional

import dev.swiftstorm.akkaradb.common.internal.binpack.AdapterSetting
import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

class MapAdapter<K, V>(
    private val keyAdapter: TypeAdapter<K>,
    private val valueAdapter: TypeAdapter<V>
) : TypeAdapter<Map<K, V>> {

    override fun estimateSize(value: Map<K, V>): Int {
        var total = 4
        for ((k, v) in value) {
            total += keyAdapter.estimateSize(k)
            total += valueAdapter.estimateSize(v)
        }
        return total
    }

    override fun write(value: Map<K, V>, buffer: ByteBuffer) {
        buffer.putInt(value.size)
        for ((k, v) in value) {
            keyAdapter.write(k, buffer)
            valueAdapter.write(v, buffer)
        }
    }

    override fun read(buffer: ByteBuffer): Map<K, V> {
        val size = buffer.int
        require(size in 0..AdapterSetting.maxCollectionSize) {
            "Collection size $size exceeds configured limit (${AdapterSetting.maxCollectionSize})"
        }
        val map = LinkedHashMap<K, V>(size)
        repeat(size) {
            val k = keyAdapter.read(buffer)
            val v = valueAdapter.read(buffer)
            map[k] = v
        }
        return map
    }
}
