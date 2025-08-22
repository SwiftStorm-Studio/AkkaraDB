package dev.swiftstorm.akkaradb.common.internal.binpack.additional

import dev.swiftstorm.akkaradb.common.internal.binpack.TypeAdapter
import java.nio.ByteBuffer

enum class EnumWidth { BYTE, SHORT, INT, AUTO }

class EnumAdapter<E : Enum<*>>(
    private val values: Array<out E>,
    width: EnumWidth = EnumWidth.AUTO,
    private val validate: Boolean = false
) : TypeAdapter<E> {

    private val chosenWidth: EnumWidth = when (width) {
        EnumWidth.AUTO -> when (values.size - 1) {
            in 0..0xFF -> EnumWidth.BYTE
            in 0x100..0xFFFF -> EnumWidth.SHORT
            else -> EnumWidth.INT
        }

        else -> width
    }

    override fun estimateSize(value: E): Int = when (chosenWidth) {
        EnumWidth.BYTE -> 1
        EnumWidth.SHORT -> 2
        EnumWidth.INT -> 4
        EnumWidth.AUTO -> error("unreachable")
    }

    override fun write(value: E, buffer: ByteBuffer) {
        val ord = value.ordinal
        when (chosenWidth) {
            EnumWidth.BYTE -> buffer.put(ord.toByte())
            EnumWidth.SHORT -> buffer.putShort(ord.toShort())
            EnumWidth.INT -> buffer.putInt(ord)
            EnumWidth.AUTO -> error("unreachable")
        }
    }

    override fun read(buffer: ByteBuffer): E {
        val ord = when (chosenWidth) {
            EnumWidth.BYTE -> (buffer.get().toInt() and 0xFF)
            EnumWidth.SHORT -> (buffer.short.toInt() and 0xFFFF)
            EnumWidth.INT -> buffer.int
            EnumWidth.AUTO -> error("unreachable")
        }
        if (validate) {
            require(ord in values.indices) { "Enum ordinal out of range: $ord / ${values.size}" }
            return values[ord]
        }
        return values[ord]
    }
}

inline fun <reified T : Enum<T>> fastEnumAdapter(
    width: EnumWidth = EnumWidth.AUTO,
    validate: Boolean = false
): EnumAdapter<T> = EnumAdapter(enumValues<T>(), width, validate)
