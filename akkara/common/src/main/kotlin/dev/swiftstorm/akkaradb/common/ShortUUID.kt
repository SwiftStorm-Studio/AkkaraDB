package dev.swiftstorm.akkaradb.common

import java.nio.ByteBuffer
import java.util.*

/**
 * Utility class for handling UUIDs and their shorter string representations.
 *
 * NOTE:
 * - This implementation serializes UUIDs in **little-endian** order via ByteBufferL.
 * - Short-string (Base64URL) values will differ from any former BIG_ENDIAN implementation.
 */
@Suppress("unused", "MemberVisibilityCanBePrivate")
class ShortUUID internal constructor(
    val uuid: UUID
) : Comparable<ShortUUID> {

    companion object {
        /**
         * Creates a random ShortUUID.
         */
        @JvmStatic
        fun generate(): ShortUUID = ShortUUID(UUID.randomUUID())

        /**
         * Creates a ShortUUID from a standard UUID string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
         */
        @JvmStatic
        fun fromUUID(uuidString: String): ShortUUID = ShortUUID(UUID.fromString(uuidString))

        /**
         * Creates a ShortUUID from a UUID.
         */
        @JvmStatic
        fun fromUUID(uuid: UUID): ShortUUID = ShortUUID(uuid)

        /**
         * Creates a ShortUUID from a short string representation (Base64 URL-safe, no padding).
         *
         * The short string must decode to exactly 16 bytes. Those 16 bytes are interpreted in
         * **little-endian** long-long order:
         *   [least significant byte first ...] for each 8-byte long.
         *
         * If you need compatibility with a legacy BIG_ENDIAN format, use `fromShortStringBE`.
         *
         * @throws IllegalArgumentException if the short string is invalid.
         */
        @JvmStatic
        fun fromShortString(shortString: String): ShortUUID {
            require(isValidShortString(shortString)) { "Invalid short string for UUID" }
            val bytes = Base64.getUrlDecoder().decode(shortString)
            // Use ByteBufferL to read little-endian longs
            val le = ByteBufferL.wrap(ByteBuffer.wrap(bytes))
            val high = le.long // LITTLE_ENDIAN due to ByteBufferL
            val low = le.long
            return ShortUUID(UUID(high, low))
        }

        /**
         * Validates if a given short string decodes to exactly 16 bytes.
         */
        @JvmStatic
        fun isValidShortString(shortString: String): Boolean = try {
            Base64.getUrlDecoder().decode(shortString).size == 16
        } catch (_: IllegalArgumentException) {
            false
        }

        /* ---------- Optional: BIG_ENDIAN compatibility readers/writers ---------- */

        /**
         * Legacy reader: interpret short string bytes as BIG_ENDIAN longs.
         * Use this only if you must load older data written with a big-endian ByteBuffer implementation.
         */
        @JvmStatic
        fun fromShortStringBE(shortString: String): ShortUUID {
            require(isValidShortString(shortString)) { "Invalid short string for UUID" }
            val bb = ByteBuffer.wrap(Base64.getUrlDecoder().decode(shortString)) // JDK default = BIG_ENDIAN
            val high = bb.long
            val low = bb.long
            return ShortUUID(UUID(high, low))
        }
    }

    /** Standard UUID string. */
    fun toUUIDString(): String = uuid.toString()

    /**
     * Short string (Base64 URL-safe without padding) using **little-endian** encoding
     * via ByteBufferL.
     *
     * If you need legacy BIG_ENDIAN output, use `toShortStringBE()`.
     */
    fun toShortString(): String {
        val le = ByteBufferL.allocate(16)
        le.putLong(uuid.mostSignificantBits) // writes as LITTLE_ENDIAN
        le.putLong(uuid.leastSignificantBits)
        val bytes = le.arrayOrCopy() // see helper extension below
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    }

    /**
     * Binary view of this UUID as a read-only **little-endian** 16-byte buffer.
     * Position = 0, Limit = 16.
     */
    fun toByteBuffer(): ByteBuffer {
        val le = ByteBufferL.allocate(16)
        le.putLong(uuid.mostSignificantBits)
        le.putLong(uuid.leastSignificantBits)
        le.flip()
        // asReadOnlyBuffer() must yield a ByteBuffer view
        return le.asReadOnlyByteBuffer()
    }

    @Deprecated("ByteArray is slow. Use toByteBuffer() instead.", ReplaceWith("toByteBuffer()"))
    fun toByteArray(): ByteArray {
        val le = ByteBufferL.allocate(16)
        le.putLong(uuid.mostSignificantBits)
        le.putLong(uuid.leastSignificantBits)
        return le.arrayOrCopy()
    }

    override fun toString(): String = toShortString()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ShortUUID) return false
        return uuid == other.uuid
    }

    override fun hashCode(): Int = uuid.hashCode()

    override fun compareTo(other: ShortUUID): Int = uuid.compareTo(other.uuid)
}

/* ============================== Extensions ============================== */

/** Converts a UUID to a ShortUUID. */
fun UUID.toShortUUID(): ShortUUID = ShortUUID(this)

/** Converts a ShortUUID to a UUID. */
fun ShortUUID.toUUID(): UUID = this.uuid

/**
 * Reads a ShortUUID from a **little-endian** 16-byte ByteBuffer.
 * Position is consumed (like ByteBuffer#getLong twice).
 */
fun ByteBuffer.toShortUUID(): ShortUUID {
    require(remaining() == 16) { "ByteBuffer must have exactly 16 bytes for UUID" }
    val le = ByteBufferL.wrap(this) // duplicates + sets LITTLE_ENDIAN
    val high = le.long
    val low = le.long
    return ShortUUID(UUID(high, low))
}

/* ============================== Helpers ============================== */
private fun ByteBufferL.arrayOrCopy(): ByteArray {
    return if (this.hasArray()) this.array()
    else {
        val dup = this.duplicate()
        val out = ByteArray(dup.remaining)
        dup.get(out)
        out
    }
}
