package dev.swiftstorm.akkaradb.common.internal.binpack

import dev.swiftstorm.akkaradb.common.internal.binpack.additional.*
import dev.swiftstorm.akkaradb.common.internal.binpack.primitive.*
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.withNullability

object AdapterResolver {
    private val adapterCache = ConcurrentHashMap<KType, TypeAdapter<*>>()

    fun getAdapterForType(type: KType): TypeAdapter<*> {
        return adapterCache.getOrPut(type) {
            val cls = type.classifier as? KClass<*>
                ?: error("Unsupported type: $type")

            // 1. Nullable
            if (type.isMarkedNullable) {
                val nonNullType = type.withNullability(false)
                val inner = getAdapterForType(nonNullType) as TypeAdapter<Any>
                return@getOrPut NullableAdapter(inner)
            }

            // 2. Primitive / Built-in
            when (cls) {
                Int::class -> return@getOrPut IntAdapter
                Long::class -> return@getOrPut LongAdapter
                Short::class -> return@getOrPut ShortAdapter
                Byte::class -> return@getOrPut ByteAdapter
                Boolean::class -> return@getOrPut BooleanAdapter
                Float::class -> return@getOrPut FloatAdapter
                Double::class -> return@getOrPut DoubleAdapter
                Char::class -> return@getOrPut CharAdapter
                String::class -> return@getOrPut StringAdapter()
                ByteArray::class -> return@getOrPut ByteArrayAdapter

                UUID::class -> return@getOrPut UUIDAdapter
                BigInteger::class -> return@getOrPut BigIntegerAdapter
                BigDecimal::class -> return@getOrPut BigDecimalAdapter
                LocalDate::class -> return@getOrPut LocalDateAdapter
                LocalTime::class -> return@getOrPut LocalTimeAdapter
                LocalDateTime::class -> return@getOrPut LocalDateTimeAdapter
                Date::class -> return@getOrPut DateAdapter
                ByteBuffer::class -> return@getOrPut ByteBufferAdapter

                else -> { /* continue to next checks */
                }
            }

            // 3. Enum
            if (cls.java.isEnum) {
                @Suppress("UNCHECKED_CAST")
                val enumClass = cls.java as Class<out Enum<*>>
                val values = enumClass.enumConstants
                    ?: error("Enum ${cls.qualifiedName} has no constants")

                @Suppress("UNCHECKED_CAST")
                return@getOrPut EnumAdapter(
                    values as Array<Enum<*>>,
                    EnumWidth.AUTO,
                    validate = false
                ) as TypeAdapter<Any>
            }


            // 4. List<T>
            if (cls == List::class) {
                val elementType = type.arguments.firstOrNull()?.type
                    ?: error("List<T> must have type argument")
                val elementAdapter = getAdapterForType(elementType)
                @Suppress("UNCHECKED_CAST")
                return@getOrPut ListAdapter(elementAdapter as TypeAdapter<Any>)
            }

            // 5. Map<K, V>
            if (cls == Map::class) {
                val (keyType, valueType) = type.arguments.mapNotNull { it.type }
                val keyAdapter = getAdapterForType(keyType)
                val valueAdapter = getAdapterForType(valueType)
                @Suppress("UNCHECKED_CAST")
                return@getOrPut MapAdapter(
                    keyAdapter as TypeAdapter<Any>,
                    valueAdapter as TypeAdapter<Any>
                )
            }

            // 6. data class
            if (cls.isData) {
                @Suppress("UNCHECKED_CAST")
                return@getOrPut generateCompositeAdapter(cls as KClass<Any>)
            }

            // 7. record class
            if (cls.java.isRecord) {
                @Suppress("UNCHECKED_CAST")
                return@getOrPut generateCompositeAdapter(cls as KClass<Any>)
            }

            error("Unsupported type: $type")
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getAdapterForClass(kClass: KClass<T>): TypeAdapter<T> {
        return getAdapterForType(kClass.createType()) as TypeAdapter<T>
    }

    fun <T : Any> generateCompositeAdapter(kClass: KClass<T>): TypeAdapter<T> {
        val constructor = kClass.primaryConstructor
            ?: error("Class ${kClass.simpleName} has no primary constructor")

        val propertyAdapters = constructor.parameters.mapIndexed { index, param ->
            val prop = kClass.memberProperties.find { it.name == param.name }
                ?: error("No matching property for constructor parameter '${param.name}'")

            val adapter = getAdapterForType(param.type)

            PropertyAdapter(prop, param, adapter as TypeAdapter<Any?>)
        }

        return object : TypeAdapter<T> {
            override fun estimateSize(value: T): Int {
                return propertyAdapters.sumOf { it.estimateSize(value) }
            }

            override fun write(value: T, buffer: ByteBuffer) {
                propertyAdapters.forEach { it.write(value, buffer) }
            }

            override fun read(buffer: ByteBuffer): T {
                val args = propertyAdapters.associate { it.param to it.read(buffer) }
                return constructor.callBy(args)
            }
        }
    }
}
