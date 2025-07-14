@file:OptIn(ExperimentalSerializationApi::class)

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.serializer
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

object AkkDSL {

    /**
     * Opens or creates a PackedTable using CBOR encoding.
     * Requires that [T] is a Kotlin data class with @Serializable and a `var id: String` field.
     */
    inline fun <reified T : Any> openPacked(
        baseDir: Path,
        format: BinaryFormat = Cbor
    ): PackedTable<T> =
        PackedTable(
            AkkaraDB.open(baseDir),
            T::class,
            serializer(),
            format
        )

    /**
     * Wrap an existing AkkaraDB instance with PackedTable.
     */
    inline fun <reified T : Any> AkkaraDB.packed(
        format: BinaryFormat = Cbor
    ): PackedTable<T> = PackedTable(this, T::class, serializer(), format)
}

class PackedTable<T : Any>(
    private val db: AkkaraDB,
    val kClass: KClass<T>,
    private val serializer: KSerializer<T>,
    private val format: BinaryFormat = Cbor,
) {
    private val ns = kClass.qualifiedName ?: kClass.simpleName!!
    private val seq = AtomicLong(0)

    private fun keyBuf(id: String): ByteBuffer =
        StandardCharsets.UTF_8.encode("$ns:$id")

    fun put(id: String, entity: T) {
        val bytes = format.encodeToByteArray(serializer, entity)
        db.put(Record(keyBuf(id), ByteBuffer.wrap(bytes), seq.incrementAndGet()))
    }

    fun get(id: String): T? = db.get(keyBuf(id))?.let { buf ->
        val arr = ByteArray(buf.remaining()).also { buf.get(it) }
        format.decodeFromByteArray(serializer, arr)
    }

    fun update(id: String, mutator: T.() -> Unit): Boolean {
        val obj = get(id) ?: return false
        mutator(obj)
        put(id, obj)
        return true
    }

    fun delete(id: String) {
        db.put(Record(keyBuf(id), ByteBuffer.allocate(0), seq.incrementAndGet()))
    }

    /**
     * Entity builder style update.
     * Requires data class [T] to have var `id: String` field to be used as primary key.
     */
    inline fun update(block: T.() -> Unit): Boolean {
        val ctor = kClass.primaryConstructor ?: error("${kClass.simpleName} must have a primary constructor")
        val instance = ctor.callBy(emptyMap())
        instance.block()

        val idProp = kClass.memberProperties.firstOrNull { it.name == "id" }
            ?: error("data class ${kClass.simpleName} must contain an 'id' property")
        val id = idProp.get(instance) as? String
            ?: error("'id' must be non-null String")

        val existing = get(id)
        if (existing == instance) return false

        put(id, instance)
        return true
    }
}
