package dev.swiftstorm.akkaradb.common.binpack

import java.util.concurrent.atomic.AtomicLong

/**
 * AdapterSetting
 *
 * Global settings for BinPack adapters and performance monitoring.
 * All settings are thread-safe and can be modified at runtime.
 */
object AdapterSetting {
    /**
     * Maximum allowed size for collections (List, Map, etc.).
     * This is a safety limit to prevent memory exhaustion from malformed data.
     * Default: 1,000,000 elements
     */
    @Volatile
    var maxCollectionSize: Int = 1_000_000
        set(value) {
            require(value > 0) { "maxCollectionSize must be positive, got $value" }
            field = value
        }

    /**
     * Maximum allowed string length in bytes.
     * This is a safety limit to prevent memory exhaustion from malformed data.
     * Default: 10 MB
     */
    @Volatile
    var maxStringLength: Int = 10 * 1024 * 1024
        set(value) {
            require(value > 0) { "maxStringLength must be positive, got $value" }
            field = value
        }

    /**
     * Enable performance statistics collection.
     * When enabled, BinPack will track encode/decode operations.
     * Default: false (disabled for performance)
     */
    @Volatile
    var enableStatistics: Boolean = false

    /**
     * Enable detailed error messages with stack traces.
     * When enabled, errors include more context but may impact performance.
     * Default: true
     */
    @Volatile
    var enableDetailedErrors: Boolean = true

    // Statistics counters (only updated when enableStatistics = true)
    @PublishedApi
    internal val encodeCount = AtomicLong(0)

    @PublishedApi
    internal val decodeCount = AtomicLong(0)

    @PublishedApi
    internal val deepCopyCount = AtomicLong(0)

    @PublishedApi
    internal val totalBytesEncoded = AtomicLong(0)

    @PublishedApi
    internal val totalBytesDecoded = AtomicLong(0)

    /**
     * Get current statistics snapshot.
     */
    fun getStatistics(): BinPackStatistics {
        return BinPackStatistics(
            encodeCount = encodeCount.get(),
            decodeCount = decodeCount.get(),
            deepCopyCount = deepCopyCount.get(),
            totalBytesEncoded = totalBytesEncoded.get(),
            totalBytesDecoded = totalBytesDecoded.get(),
            customAdapterCount = AdapterRegistry.getStats().customAdapterCount
        )
    }

    /**
     * Reset all statistics counters to zero.
     */
    fun resetStatistics() {
        encodeCount.set(0)
        decodeCount.set(0)
        deepCopyCount.set(0)
        totalBytesEncoded.set(0)
        totalBytesDecoded.set(0)
    }
}

/**
 * Statistics snapshot for BinPack operations.
 */
data class BinPackStatistics(
    val encodeCount: Long,
    val decodeCount: Long,
    val deepCopyCount: Long,
    val totalBytesEncoded: Long,
    val totalBytesDecoded: Long,
    val customAdapterCount: Int
) {
    val averageBytesPerEncode: Double
        get() = if (encodeCount > 0) totalBytesEncoded.toDouble() / encodeCount else 0.0

    val averageBytesPerDecode: Double
        get() = if (decodeCount > 0) totalBytesDecoded.toDouble() / decodeCount else 0.0

    override fun toString(): String = buildString {
        appendLine("BinPack Statistics:")
        appendLine("  Encode operations: $encodeCount")
        appendLine("  Decode operations: $decodeCount")
        appendLine("  Deep copy operations: $deepCopyCount")
        appendLine("  Total bytes encoded: $totalBytesEncoded")
        appendLine("  Total bytes decoded: $totalBytesDecoded")
        appendLine("  Average bytes per encode: ${"%.2f".format(averageBytesPerEncode)}")
        appendLine("  Average bytes per decode: ${"%.2f".format(averageBytesPerDecode)}")
        appendLine("  Custom adapters registered: $customAdapterCount")
    }
}