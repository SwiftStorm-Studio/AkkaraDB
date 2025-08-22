package dev.swiftstorm.akkaradb.common.internal.binpack.primitive

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer
import java.util.*

object UUIDAdapter : TypeAdapter<UUID> {
    override fun estimateSize(value: UUID) = 16 // 2 * Long.SIZE_BYTES
    override fun write(value: UUID, buffer: ByteBuffer) {
        buffer.putLong(value.mostSignificantBits)
        buffer.putLong(value.leastSignificantBits)
    }

    override fun read(buffer: ByteBuffer): UUID {
        val mostSigBits = buffer.long
        val leastSigBits = buffer.long
        return UUID(mostSigBits, leastSigBits)
    }
}