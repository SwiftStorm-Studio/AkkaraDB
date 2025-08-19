package dev.swiftstorm.akkaradb.engine.util

import java.nio.ByteBuffer
import java.util.*

/**
 * Utility class for handling UUIDs and their shorter string representations.
 */
@Suppress("unused", "MemberVisibilityCanBePrivate")
class ShortUUID internal constructor(
    val uuid: UUID
) : Comparable<ShortUUID> {
    companion object {
        /**
         * Creates a random ShortUUID.
         * It contains a random UUID.
         * @return A new ShortUUID instance.
         */
        @JvmStatic
        fun generate(): ShortUUID = ShortUUID(UUID.randomUUID())

        /**
         * Creates a ShortUUID from a standard UUID string.
         * @param uuidString The UUID string.
         * @return A new ShortUUID instance.
         */
        @JvmStatic
        fun fromUUID(uuidString: String): ShortUUID = ShortUUID(UUID.fromString(uuidString))

        /**
         * Creates a ShortUUID from a UUID.
         * @param uuid The UUID.
         * @return A new ShortUUID instance.
         */
        @JvmStatic
        fun fromUUID(uuid: UUID): ShortUUID = ShortUUID(uuid)

        /**
         * Creates a ShortUUID from a short string representation.
         * @param shortString The short string representation of the UUID.
         * @return A new ShortUUID instance.
         * @throws IllegalArgumentException if the short string is invalid.
         */
        @JvmStatic
        fun fromShortString(shortString: String): ShortUUID {
            require(isValidShortString(shortString)) { "Invalid short string for UUID" }
            val bytes = Base64.getUrlDecoder().decode(shortString)
            val bb = ByteBuffer.wrap(bytes)
            val high = bb.long
            val low = bb.long
            return ShortUUID(UUID(high, low))
        }

        /**
         * Validates if a given short string is a valid short UUID.
         * @param shortString The short string to validate.
         * @return True if the short string is valid, false otherwise.
         */
        @JvmStatic
        fun isValidShortString(shortString: String): Boolean = try {
            Base64.getUrlDecoder().decode(shortString).size == 16
        } catch (e: IllegalArgumentException) {
            false
        }
    }

    /**
     * Converts the UUID to a standard string representation.
     * @return The standard UUID string.
     */
    fun toUUIDString(): String = uuid.toString()

    /**
     * Converts the UUID to a short string representation.
     * @return The short string representation of the UUID.
     */
    fun toShortString(): String {
        val bb = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bb.array())
    }

    /**
     * Converts the UUID to a byte array.
     * @return The byte array representation of the UUID.
     */
    @Deprecated("ByteArray is slow. Use toByteBuffer() instead.", ReplaceWith("toByteBuffer()"))
    fun toByteArray(): ByteArray {
        val bb = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        return bb.array()
    }

    /**
     * Converts the UUID to a ByteBuffer (16 bytes).
     * This is useful when you want to use the raw UUID as a binary key
     * without going through string encoding.
     *
     * The returned buffer is read-only and positioned at 0.
     */
    fun toByteBuffer(): ByteBuffer {
        val bb = ByteBuffer.allocate(16)
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        bb.flip() // reset position to 0 for reading
        return bb.asReadOnlyBuffer()
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

/**
 * Converts a UUID to a ShortUUID.
 * @return The ShortUUID representation of the UUID.
 */
fun UUID.toShortUUID(): ShortUUID = ShortUUID(this)

/**
 * Converts a ShortUUID to a UUID.
 * @return The UUID representation of the ShortUUID.
 */
fun ShortUUID.toUUID(): UUID = UUID.fromString(toUUIDString())

/**
 * Converts a ByteBuffer (16 bytes) to a ShortUUID.
 * @return The ShortUUID representation of the UUID in the ByteBuffer.
 * @throws IllegalArgumentException if the ByteBuffer does not have exactly 16 bytes.
 */
fun ByteBuffer.toShortUUID(): ShortUUID {
    require(remaining() == 16) { "ByteBuffer must have exactly 16 bytes for UUID" }
    val high = long
    val low = long
    return ShortUUID(UUID(high, low))
}