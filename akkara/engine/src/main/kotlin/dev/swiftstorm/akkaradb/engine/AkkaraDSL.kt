@file:OptIn(ExperimentalSerializationApi::class)

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.serializer
import net.ririfa.binpack.AdapterResolver
import net.ririfa.binpack.BinPack
import net.ririfa.binpack.BinPackBufferPool
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

    private val nsBuf: ByteBuffer by lazy {
        StandardCharsets.UTF_8.encode("${kClass.qualifiedName ?: kClass.simpleName!!}:").asReadOnlyBuffer()
    }

    private val seq = AtomicLong(seqSeed ?: db.lastSeq())

    /* ───────── constants ───────── */

    private val TOMBSTONE: ByteBuffer = ByteBuffer.allocate(0).asReadOnlyBuffer()
    private val objenesis by lazy { ObjenesisStd(true) }

    /* ───────── key helpers ───────── */

    /**
     * Allocates a *heap* ByteBuffer for the logical key and returns it as read‑only.
     *
     * @throws IllegalArgumentException if [id] is not ASCII.
     */
    // AkkaraDSL.kt -> PackedTable.keyBuf()
    private fun keyBuf(id: String): ByteBuffer {
        if (!id.isASCII()) error("ID must be ASCII: $id")
        val rNsBuf = nsBuf.duplicate().apply { rewind() }
        val idBuf = StandardCharsets.UTF_8.encode(id)
        val out = ByteBuffer.allocate(rNsBuf.remaining() + idBuf.remaining())
        out.put(rNsBuf).put(idBuf)
        out.flip()
        return out.asReadOnlyBuffer()
    }


    /* ───────── CRUD ───────── */

    /**
     * Inserts or overwrites the entity under [id].
     * INSERT ... ON CONFLICT DO UPDATE SET .**/
    fun put(id: String, entity: T) {
        if (!id.isASCII()) {
            throw IllegalArgumentException("ID must be ASCII: $id")
        }

        val encoded = BinPack.encode(kClass, entity)
        val key = keyBuf(id)
        val seqNum = seq.incrementAndGet()

        try {
            db.put(Record(key, encoded, seqNum))
        } finally {
            BinPackBufferPool.recycle(encoded) // return the buffer to the pool
        }
    }

    /** Retrieves the entity stored under [id], or `null` if absent or tombstoned. */
    fun get(id: String): T? = db.get(keyBuf(id))?.let { buf ->
        if (buf.remaining() == 0) return null

        BinPack.decode(kClass, buf)
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

    private fun T.deepCopy(): T {
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        return adapter.copy(this)
    }
}

/* ───────── extension ───────── */

fun String.isASCII(): Boolean = all { it.code <= 127 }

fun <T : Any> BinPack.encode(kClass: KClass<T>, value: T): ByteBuffer {
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    val size = adapter.estimateSize(value)
    val buffer = BinPackBufferPool.borrow(size)
    adapter.write(value, buffer)
    buffer.flip()
    return buffer
}

fun <T : Any> BinPack.decode(kClass: KClass<T>, buffer: ByteBuffer): T {
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    return adapter.read(buffer)
}