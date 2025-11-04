package dev.swiftstorm.akkaradb.common.binpack.additional

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterSetting
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

/**
 * Adapter for Map<K,V> with prefixed size.
 * Encoding: [i32 size][(key,value)...]
 */
class MapAdapter<K, V>(
    private val keyAdapter: TypeAdapter<K>,
    private val valueAdapter: TypeAdapter<V>
) : TypeAdapter<Map<K, V>> {

    override fun estimateSize(value: Map<K, V>): Int {
        var total = 4 // size header
        for ((k, v) in value) {
            total += keyAdapter.estimateSize(k)
            total += valueAdapter.estimateSize(v)
        }
        return total
    }

    override fun write(value: Map<K, V>, buffer: ByteBufferL) {
        buffer.i32 = value.size
        for ((k, v) in value) {
            keyAdapter.write(k, buffer)
            valueAdapter.write(v, buffer)
        }
    }

    override fun read(buffer: ByteBufferL): Map<K, V> {
        val size = buffer.i32
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