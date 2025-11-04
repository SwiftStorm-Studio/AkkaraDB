package dev.swiftstorm.akkaradb.common.binpack

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.additional.*
import dev.swiftstorm.akkaradb.common.binpack.primitive.*
import java.math.BigDecimal
import java.math.BigInteger
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

/**
 * AdapterResolver (ByteBufferL-only)
 *
 * Resolves a TypeAdapter for a given KType. Public interface remains unchanged
 * but all adapters are expected to operate on ByteBufferL.
 *
 * NOTE:
 * - Primitive and additional adapters must be ByteBufferL-based implementations.
 * - ByteBuffer-based adapters are not supported anymore.
 */
object AdapterResolver {
    private val adapterCache = ConcurrentHashMap<KType, TypeAdapter<*>>()

    fun getAdapterForType(type: KType): TypeAdapter<*> {
        return adapterCache.getOrPut(type) {
            val cls = type.classifier as? KClass<*>
                ?: error("Unsupported type: $type")

            // 1. Nullable<T?>
            if (type.isMarkedNullable) {
                val nonNullType = type.withNullability(false)

                @Suppress("UNCHECKED_CAST")
                val inner = getAdapterForType(nonNullType) as TypeAdapter<Any>
                return@getOrPut NullableAdapter(inner)
            }

            // 2. Primitive / Built-in (these must be L-based in your codebase)
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

            // 6. data class / record class (generate composite adapter on the fly)
            if (cls.isData || cls.java.isRecord) {
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

    /**
     * Reflection-based composite adapter for Kotlin data classes and Java records.
     * Uses ByteBufferL-only adapters for fields.
     */
    fun <T : Any> generateCompositeAdapter(kClass: KClass<T>): TypeAdapter<T> {
        val constructor = kClass.primaryConstructor
            ?: error("Class ${kClass.simpleName} has no primary constructor")

        // Map constructor params to matching properties and adapters
        data class Field(val name: String, val getter: (Any) -> Any?, val adapter: TypeAdapter<Any?>, val param: kotlin.reflect.KParameter)

        val propsByName = kClass.memberProperties.associateBy { it.name }
        val fields = constructor.parameters.map { param ->
            val prop = propsByName[param.name]
                ?: error("No matching property for constructor parameter '${param.name}'")

            @Suppress("UNCHECKED_CAST")
            val getter: (Any) -> Any? = { instance -> (prop as kotlin.reflect.KProperty1<Any, Any?>).get(instance) }

            @Suppress("UNCHECKED_CAST")
            val adapter = getAdapterForType(param.type) as TypeAdapter<Any?>
            Field(param.name ?: "<unnamed>", getter, adapter, param)
        }

        return object : TypeAdapter<T> {
            override fun estimateSize(value: T): Int {
                var sum = 0
                for (f in fields) sum += f.adapter.estimateSize(f.getter(value))
                return sum
            }

            override fun write(value: T, buffer: ByteBufferL) {
                for (f in fields) {
                    @Suppress("UNCHECKED_CAST")
                    f.adapter.write(f.getter(value), buffer)
                }
            }

            override fun read(buffer: ByteBufferL): T {
                val args = fields.associate { f ->
                    f.param to f.adapter.read(buffer)
                }
                return constructor.callBy(args)
            }
        }
    }
}
