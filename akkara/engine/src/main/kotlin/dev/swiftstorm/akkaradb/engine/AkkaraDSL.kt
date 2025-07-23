@file:OptIn(ExperimentalSerializationApi::class)

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.serializer
import org.objenesis.ObjenesisStd
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * DSL entry‑point helpers for AkkaraDB.
 */
object AkkDSL {

    /* ───────── factory helpers ───────── */
    inline fun <reified T : Any> openPacked(
        baseDir: Path,
        seqSeed: Long? = null,
        format: BinaryFormat = Cbor,
    ): PackedTable<T> = PackedTable(
        AkkaraDB.open(baseDir),
        T::class,
        serializer(),
        format,
        seqSeed
    )

    inline fun <reified T : Any> AkkaraDB.packed(
        format: BinaryFormat = Cbor,
        seqSeed: Long? = null,
    ): PackedTable<T> = PackedTable(this, T::class, serializer(), format, seqSeed)
}

/**
 * Type‑safe wrapper around AkkaraDB that provides CRUD / UPSERT helpers for a single entity type [T].
 */
class PackedTable<T : Any>(
    val db: AkkaraDB,
    private val kClass: KClass<T>,
    private val serializer: KSerializer<T>,
    private val format: BinaryFormat = Cbor,
    seqSeed: Long? = null,
) {

    /* ───────── namespace & seq ───────── */

    private val nsBytes: ByteArray =
        "${kClass.qualifiedName ?: kClass.simpleName!!}:".toByteArray(StandardCharsets.UTF_8)

    private val seq = AtomicLong(seqSeed ?: db.bestEffortLastSeq())

    /* ───────── constants ───────── */

    private val TOMBSTONE: ByteBuffer = ByteBuffer.allocate(0).asReadOnlyBuffer()
    private val objenesis by lazy { ObjenesisStd(true) }

    /* ───────── key helpers ───────── */

    /**
     * Allocates a *heap* ByteBuffer for the logical key and returns it as read‑only.
     */
    private fun keyBuf(id: String): ByteBuffer {
        val idBytes = id.toByteArray(StandardCharsets.UTF_8)
        val buf = ByteBuffer.allocate(nsBytes.size + idBytes.size)
        buf.put(nsBytes).put(idBytes).flip()
        return buf.asReadOnlyBuffer()
    }

    /* ───────── CRUD ───────── */

    /**
     * Inserts or overwrites the entity under [id].
     * INSERT ... ON CONFLICT DO UPDATE SET ...
     **/
    fun put(id: String, entity: T) {
        val bytes = format.encodeToByteArray(serializer, entity)
        require(bytes.isNotEmpty()) { "Entity encodes to 0 bytes; would collide with tombstone" }
        db.put(Record(keyBuf(id), ByteBuffer.wrap(bytes), seq.incrementAndGet()))
    }

    /** Retrieves the entity stored under [id], or `null` if absent or tombstoned. */
    fun get(id: String): T? = db.get(keyBuf(id))?.let { buf ->
        if (buf.remaining() == 0) return null          // tombstone
        val arr = ByteArray(buf.remaining()).also { buf.get(it) }
        format.decodeFromByteArray(serializer, arr)
    }

    /** Writes a tombstone for [id] (logical delete). */
    fun delete(id: String) {
        db.put(Record(keyBuf(id), TOMBSTONE, seq.incrementAndGet()))
    }

    /* ───────── Higher‑order helpers ───────── */

    /**
     * Read‑modify‑write loop. Returns `true` if an *actual change* was persisted.
     */
    fun update(id: String, mutator: T.() -> Unit): Boolean {
        while (true) {
            val old = get(id) ?: return false
            val copy = old.deepCopy()
            mutator(copy)
            if (copy == old) return false
            put(id, copy)
            return true
        }
    }

    /**
     * UPSERT helper analogous to SQL's `INSERT … ON DUPLICATE KEY UPDATE`.
     */
    fun upsert(id: String, init: T.() -> Unit): T {
        val entity: T = get(id) ?: run {
            val ctor0 = kClass.constructors.firstOrNull { it.parameters.isEmpty() }
            (ctor0?.call() ?: objenesis.newInstance(kClass.java))
        }

        entity.init()

        // NOT‑NULL & default value check
        for (prop in kClass.memberProperties) {
            prop.isAccessible = true
            val value = prop.get(entity)
            if (!prop.returnType.isMarkedNullable) {
                when (value) {
                    null -> error("Property '${prop.name}' must be set (null)")
                    is Int -> if (value == 0) error("Property '${prop.name}' must be set (0)")
                    is Long -> if (value == 0L) error("Property '${prop.name}' must be set (0L)")
                    is Boolean -> if (!value) error("Property '${prop.name}' must be set (false)")
                    is String -> if (value.isEmpty()) error("Property '${prop.name}' must be set (empty)")
                    else -> {}
                }
            }
        }

        put(id, entity)
        return entity
    }

    /* ───────── internals ───────── */

    private fun T.deepCopy(): T =
        format.decodeFromByteArray(serializer, format.encodeToByteArray(serializer, this))
}

/* ───────── extension ───────── */

private fun AkkaraDB.bestEffortLastSeq(): Long =
    runCatching { javaClass.getMethod("lastSeq").invoke(this) as? Long ?: 0L }.getOrElse { 0L }
