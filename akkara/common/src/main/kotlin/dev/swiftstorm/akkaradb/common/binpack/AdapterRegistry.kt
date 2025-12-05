package dev.swiftstorm.akkaradb.common.binpack

import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType

/**
 * AdapterRegistry
 *
 * Manages custom adapter registration and provides fast lookup.
 * Custom adapters take priority over built-in adapters.
 *
 * Thread-safe: All operations are thread-safe and lock-free where possible.
 */
object AdapterRegistry {
    // Custom adapters registered by the user (take priority)
    private val customAdaptersByType = ConcurrentHashMap<KType, TypeAdapter<*>>()
    private val customAdaptersByClass = ConcurrentHashMap<KClass<*>, TypeAdapter<*>>()

    /**
     * Register a custom adapter for a specific type.
     * This adapter will be used instead of the built-in adapter.
     *
     * @param type The KType to register the adapter for
     * @param adapter The adapter instance
     * @throws IllegalArgumentException if adapter is null
     */
    fun <T : Any> registerAdapter(type: KType, adapter: TypeAdapter<T>) {
        customAdaptersByType[type] = adapter
    }

    /**
     * Register a custom adapter for a specific class.
     * This is a convenience method that creates a non-nullable type from the class.
     *
     * @param kClass The KClass to register the adapter for
     * @param adapter The adapter instance
     */
    fun <T : Any> registerAdapter(kClass: KClass<T>, adapter: TypeAdapter<T>) {
        val type = kClass.createType(nullable = false)
        customAdaptersByType[type] = adapter
        customAdaptersByClass[kClass] = adapter
    }

    /**
     * Register a custom adapter using reified type parameter.
     * This is the most convenient method for Kotlin users.
     *
     * Example:
     * ```
     * AdapterRegistry.register<MyClass>(MyClassAdapter())
     * ```
     */
    inline fun <reified T : Any> register(adapter: TypeAdapter<T>) {
        registerAdapter(T::class, adapter)
    }

    /**
     * Unregister a custom adapter for a specific type.
     * Returns the previously registered adapter, or null if none was registered.
     */
    fun unregisterAdapter(type: KType): TypeAdapter<*>? {
        return customAdaptersByType.remove(type)
    }

    /**
     * Unregister a custom adapter for a specific class.
     */
    fun <T : Any> unregisterAdapter(kClass: KClass<T>): TypeAdapter<*>? {
        val type = kClass.createType(nullable = false)
        customAdaptersByClass.remove(kClass)
        return customAdaptersByType.remove(type)
    }

    /**
     * Clear all custom adapters.
     */
    fun clearAll() {
        customAdaptersByType.clear()
        customAdaptersByClass.clear()
    }

    /**
     * Check if a custom adapter is registered for the given type.
     */
    fun hasCustomAdapter(type: KType): Boolean {
        return customAdaptersByType.containsKey(type)
    }

    /**
     * Check if a custom adapter is registered for the given class.
     */
    fun hasCustomAdapter(kClass: KClass<*>): Boolean {
        return customAdaptersByClass.containsKey(kClass)
    }

    /**
     * Get a custom adapter for the given type, or null if none is registered.
     * This is used internally by AdapterResolver.
     */
    @PublishedApi
    internal fun getCustomAdapter(type: KType): TypeAdapter<*>? {
        return customAdaptersByType[type]
    }

    /**
     * Get a custom adapter for the given class, or null if none is registered.
     * Fast path for class-based lookups.
     */
    @PublishedApi
    internal fun getCustomAdapter(kClass: KClass<*>): TypeAdapter<*>? {
        return customAdaptersByClass[kClass]
    }

    /**
     * Get statistics about registered adapters.
     */
    fun getStats(): AdapterStats {
        return AdapterStats(
            customAdapterCount = customAdaptersByType.size,
            customClassAdapterCount = customAdaptersByClass.size
        )
    }
}

/**
 * Statistics about registered adapters.
 */
data class AdapterStats(
    val customAdapterCount: Int,
    val customClassAdapterCount: Int
)
