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
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.isAccessible

/**
 * AdapterResolver (ByteBufferL-only, v1.0.0)
 *
 * Resolves a TypeAdapter for a given KType with support for custom adapters.
 * Custom adapters registered via AdapterRegistry take priority over built-in adapters.
 *
 * Performance optimizations:
 * - Fast path for primitive types
 * - Efficient caching with computeIfAbsent
 * - Custom adapter priority system
 * - Minimized reflection overhead
 *
 * NOTE:
 * - All adapters must be ByteBufferL-based implementations.
 * - ByteBuffer-based adapters are not supported.
 */
object AdapterResolver {
    private val adapterCache = ConcurrentHashMap<KType, TypeAdapter<*>>()

    /**
     * Get an adapter for the given type.
     * Checks custom adapters first, then falls back to built-in adapters.
     */
    fun getAdapterForType(type: KType): TypeAdapter<*> {
        adapterCache[type]?.let { return it }

        // resolveAdapter を先に呼んで、その結果をキャッシュに入れる
        val adapter = resolveAdapter(type)
        return adapterCache.putIfAbsent(type, adapter) ?: adapter
    }

    /**
     * Internal adapter resolution logic.
     * This is separated to make caching logic clearer.
     */
    private fun resolveAdapter(type: KType): TypeAdapter<*> {
        // 1. Check for custom adapter first (highest priority)
        AdapterRegistry.getCustomAdapter(type)?.let { return it }

        val cls = type.classifier as? KClass<*>
            ?: error("Unsupported type: $type (classifier is not a KClass)")

        // 2. Nullable<T?>
        if (type.isMarkedNullable) {
            val nonNullType = type.withNullability(false)

            @Suppress("UNCHECKED_CAST")
            val inner = getAdapterForType(nonNullType) as TypeAdapter<Any>
            return NullableAdapter(inner)
        }

        // 3. Check for custom adapter by class (second priority)
        AdapterRegistry.getCustomAdapter(cls)?.let { return it }

        // 4. Primitive types (fast path - most common case)
        // Optimized: early return for primitives to avoid further checks
        when (cls) {
            Int::class -> return IntAdapter
            Long::class -> return LongAdapter
            Short::class -> return ShortAdapter
            Byte::class -> return ByteAdapter
            Boolean::class -> return BooleanAdapter
            Float::class -> return FloatAdapter
            Double::class -> return DoubleAdapter
            Char::class -> return CharAdapter
        }

        // 5. Common built-in types
        when (cls) {
            String::class -> return StringAdapter()
            ByteArray::class -> return ByteArrayAdapter

            UUID::class -> return UUIDAdapter
            BigInteger::class -> return BigIntegerAdapter
            BigDecimal::class -> return BigDecimalAdapter
            LocalDate::class -> return LocalDateAdapter
            LocalTime::class -> return LocalTimeAdapter
            LocalDateTime::class -> return LocalDateTimeAdapter
            Date::class -> return DateAdapter
        }

        // 6. Enum
        if (cls.java.isEnum) {
            @Suppress("UNCHECKED_CAST")
            val enumClass = cls.java as Class<out Enum<*>>
            val values = enumClass.enumConstants
                ?: error("Enum ${cls.qualifiedName} has no constants")

            @Suppress("UNCHECKED_CAST")
            return EnumAdapter(
                values as Array<Enum<*>>,
                EnumWidth.AUTO,
                validate = false
            ) as TypeAdapter<Any>
        }

        // 7. List<T>
        if (cls == List::class) {
            val elementType = type.arguments.firstOrNull()?.type
                ?: error("List<T> must have type argument")
            val elementAdapter = getAdapterForType(elementType)
            @Suppress("UNCHECKED_CAST")
            return ListAdapter(elementAdapter as TypeAdapter<Any>)
        }

        // 8. Map<K, V>
        if (cls == Map::class) {
            val typeArgs = type.arguments.mapNotNull { it.type }
            if (typeArgs.size != 2) {
                error("Map<K, V> must have exactly 2 type arguments, got ${typeArgs.size}")
            }
            val (keyType, valueType) = typeArgs
            val keyAdapter = getAdapterForType(keyType)
            val valueAdapter = getAdapterForType(valueType)
            @Suppress("UNCHECKED_CAST")
            return MapAdapter(
                keyAdapter as TypeAdapter<Any>,
                valueAdapter as TypeAdapter<Any>
            )
        }

        // 9. Data class / Record class (reflection-based composite adapter)
        if (cls.isData || cls.java.isRecord) {
            @Suppress("UNCHECKED_CAST")
            return generateCompositeAdapter(cls as KClass<Any>)
        }

        error("Unsupported type: $type (no adapter registered and no built-in support)")
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getAdapterForClass(kClass: KClass<T>): TypeAdapter<T> {
        return getAdapterForType(kClass.createType()) as TypeAdapter<T>
    }

    /**
     * Reflection-based composite adapter for Kotlin data classes and Java records.
     * Uses ByteBufferL-only adapters for fields.
     *
     * Optimizations:
     * - Pre-compute field metadata to minimize reflection overhead
     * - Use array-based field storage for faster iteration
     * - Cache property accessors
     */
    fun <T : Any> generateCompositeAdapter(kClass: KClass<T>): TypeAdapter<T> {
        val constructor = kClass.primaryConstructor
            ?: error("Class ${kClass.qualifiedName ?: kClass.simpleName} has no primary constructor")

        // Validate constructor is callable
        if (!constructor.isAccessible) {
            try {
                constructor.isAccessible = true
            } catch (e: Exception) {
                error("Cannot make constructor accessible for ${kClass.qualifiedName}: ${e.message}")
            }
        }

        // Pre-compute field metadata for performance
        data class FieldMeta(
            val name: String,
            val getter: (Any) -> Any?,
            val adapter: TypeAdapter<Any?>,
            val param: KParameter
        )

        val propsByName = kClass.memberProperties.associateBy { it.name }
        val fieldMetaArray = constructor.parameters.map { param ->
            val paramName = param.name
                ?: error("Constructor parameter has no name in ${kClass.qualifiedName}")

            val prop = propsByName[paramName]
                ?: error("No matching property for constructor parameter '$paramName' in ${kClass.qualifiedName}")

            // Make property accessible if needed
            if (!prop.isAccessible) {
                try {
                    prop.isAccessible = true
                } catch (e: Exception) {
                    // Ignore if we can't make it accessible, try anyway
                }
            }

            @Suppress("UNCHECKED_CAST")
            val getter: (Any) -> Any? = { instance ->
                try {
                    (prop as KProperty1<Any, Any?>).get(instance)
                } catch (e: Exception) {
                    throw BinPackFormatException(
                        "Failed to get property '$paramName' from ${kClass.qualifiedName}: ${e.message}",
                        e
                    )
                }
            }

            @Suppress("UNCHECKED_CAST")
            val adapter = try {
                getAdapterForType(param.type) as TypeAdapter<Any?>
            } catch (e: Exception) {
                throw BinPackFormatException(
                    "Failed to resolve adapter for parameter '$paramName' of type ${param.type} in ${kClass.qualifiedName}: ${e.message}",
                    e
                )
            }

            FieldMeta(paramName, getter, adapter, param)
        }.toTypedArray() // Use array for faster iteration

        val fieldCount = fieldMetaArray.size

        return object : TypeAdapter<T> {
            override fun estimateSize(value: T): Int {
                var sum = 0
                // Use indices for slightly better performance
                for (i in 0 until fieldCount) {
                    val f = fieldMetaArray[i]
                    sum += f.adapter.estimateSize(f.getter(value))
                }
                return sum
            }

            override fun write(value: T, buffer: ByteBufferL) {
                // Use indices for slightly better performance
                for (i in 0 until fieldCount) {
                    val f = fieldMetaArray[i]
                    f.adapter.write(f.getter(value), buffer)
                }
            }

            override fun read(buffer: ByteBufferL): T {
                // Build argument map for constructor
                val args = HashMap<KParameter, Any?>(fieldCount)
                for (i in 0 until fieldCount) {
                    val f = fieldMetaArray[i]
                    try {
                        val fieldValue = f.adapter.read(buffer)
                        args[f.param] = fieldValue
                    } catch (e: Exception) {
                        throw BinPackFormatException(
                            "Failed to read field '${f.name}' in ${kClass.qualifiedName}: ${e.message}",
                            e
                        )
                    }
                }

                return try {
                    constructor.callBy(args)
                } catch (e: Exception) {
                    throw BinPackFormatException(
                        "Failed to construct instance of ${kClass.qualifiedName}: ${e.message}",
                        e
                    )
                }
            }
        }
    }
}
