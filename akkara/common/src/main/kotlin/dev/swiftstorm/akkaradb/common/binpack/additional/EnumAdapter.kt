package dev.swiftstorm.akkaradb.common.binpack.additional

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter

enum class EnumWidth { BYTE, SHORT, INT, AUTO }

/**
 * Adapter for compact enum serialization by ordinal with selectable width.
 * Encoding:
 *   - BYTE  : [i8  ordinal]
 *   - SHORT : [i16 ordinal]
 *   - INT   : [i32 ordinal]
 */
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

    override fun write(value: E, buffer: ByteBufferL) {
        val ord = value.ordinal
        if (validate) {
            when (chosenWidth) {
                EnumWidth.BYTE -> require(ord in 0..0xFF) { "Enum ordinal out of BYTE range: $ord" }
                EnumWidth.SHORT -> require(ord in 0..0xFFFF) { "Enum ordinal out of SHORT range: $ord" }
                EnumWidth.INT -> { /* always fits */
                }

                EnumWidth.AUTO -> error("unreachable")
            }
        }
        when (chosenWidth) {
            EnumWidth.BYTE -> buffer.i8 = ord
            EnumWidth.SHORT -> buffer.i16 = ord.toShort()
            EnumWidth.INT -> buffer.i32 = ord
            EnumWidth.AUTO -> error("unreachable")
        }
    }

    override fun read(buffer: ByteBufferL): E {
        val ord = when (chosenWidth) {
            EnumWidth.BYTE -> buffer.i8 and 0xFF
            EnumWidth.SHORT -> buffer.i16.toInt() and 0xFFFF
            EnumWidth.INT -> buffer.i32
            EnumWidth.AUTO -> error("unreachable")
        }
        if (validate) require(ord in values.indices) { "Enum ordinal out of range: $ord / ${values.size}" }
        return values[ord]
    }
}

inline fun <reified T : Enum<T>> fastEnumAdapter(
    width: EnumWidth = EnumWidth.AUTO,
    validate: Boolean = false
): EnumAdapter<T> = EnumAdapter(enumValues<T>(), width, validate)
