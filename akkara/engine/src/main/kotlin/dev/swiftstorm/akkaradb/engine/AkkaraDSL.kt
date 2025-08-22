@file:Suppress("unused")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.internal.binpack.AdapterResolver
import dev.swiftstorm.akkaradb.common.internal.binpack.BinPack
import dev.swiftstorm.akkaradb.common.internal.binpack.BinPackBufferPool
import dev.swiftstorm.akkaradb.engine.util.ShortUUID
import org.objenesis.ObjenesisStd
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * DSL entry-point helpers for AkkaraDB.
 */
object AkkDSL {
    /* ───────── factory helpers ───────── */
    inline fun <reified T : Any> open(baseDir: Path): PackedTable<T> =
        PackedTable(
            AkkaraDB.open(baseDir),
            T::class
        )

    inline fun <reified T : Any> AkkaraDB.open(): PackedTable<T> =
        PackedTable(
            this,
            T::class
        )

    inline fun <T : Any> PackedTable<T>.close() {
        db.close()
    }
}

/**
 * Type-safe wrapper around AkkaraDB that provides CRUD / UPSERT helpers for a single entity type [T].
 */
class PackedTable<T : Any>(
    val db: AkkaraDB,
    private val kClass: KClass<T>
) : Closeable {

    override fun close() {
        db.close()
    }

    /* ───────── namespace & seq ───────── */

    private val nsBuf: ByteBuffer by lazy {
        StandardCharsets.US_ASCII
            .encode("${kClass.qualifiedName ?: kClass.simpleName!!}:")
            .asReadOnlyBuffer()
    }

    /* ───────── constants ───────── */

    private val objenesis by lazy { ObjenesisStd(true) }

    companion object {
        private const val SEP: Char = 0x1F.toChar() // ASCII 0x1F (Unit Separator)
        private val EMPTY: ByteBuffer = ByteBuffer.allocate(0).asReadOnlyBuffer() // 0B empty buffer
    }

    /* ───────── key helpers ───────── */

    private fun keyBuf(id: String, uuid: ShortUUID): ByteBuffer {
        require(id.isAsciiNoSep()) { "ID must be ASCII and must not contain 0x1F: $id" }

        val rNs = nsBuf.duplicate().apply { rewind() }
        val idBuf = StandardCharsets.US_ASCII.encode(id)
        val uuidBuf = uuid.toByteBuffer()                       // 16B

        val out = ByteBuffer.allocate(rNs.remaining() + idBuf.remaining() + 1 + uuidBuf.remaining())
        out.put(rNs).put(idBuf).put(SEP.code.toByte()).put(uuidBuf)
        out.flip()
        return out.asReadOnlyBuffer()
    }

    private fun prefixBuf(id: String): ByteBuffer {
        require(id.isAsciiNoSep())
        val rNs = nsBuf.duplicate().apply { rewind() }
        val idBuf = StandardCharsets.US_ASCII.encode(id)
        val out = ByteBuffer.allocate(rNs.remaining() + idBuf.remaining())
        out.put(rNs).put(idBuf)
        out.flip()
        return out.asReadOnlyBuffer()
    }

    private fun String.isAsciiNoSep() =
        all { it.code in 0..0x7F } && indexOf(SEP) == -1

    /* ───────── CRUD ───────── */

    fun put(id: String, uuid: ShortUUID, entity: T) {
        val encoded = BinPack.encode(kClass, entity)
        val key = keyBuf(id, uuid)
        val seqNum = db.nextSeq()
        try {
            db.put(Record(key, encoded, seqNum))
        } finally {
            BinPackBufferPool.release(encoded)
        }
    }

    fun get(id: String, uuid: ShortUUID): T? {
        val key = keyBuf(id, uuid)
        return db.get(key)?.let { rec ->
            if (rec.isTombstone) null else BinPack.decode(kClass, rec.value)
        }
    }

    fun delete(id: String, uuid: ShortUUID) {
        val seqNum = db.nextSeq()
        val rec = Record(keyBuf(id, uuid), EMPTY, seqNum).asTombstone()
        db.put(rec)
    }

    /* ───────── Higher-order helpers ───────── */

    fun update(id: String, uuid: ShortUUID, mutator: T.() -> Unit): Boolean {
        val old = get(id, uuid) ?: return false
        val copy = old.deepCopy().apply(mutator)
        if (copy == old) return false
        put(id, uuid, copy)
        return true
    }

    fun upsert(id: String, uuid: ShortUUID, init: T.() -> Unit): T {
        val entity: T = get(id, uuid) ?: run {
            val ctor0 = kClass.constructors.firstOrNull { it.parameters.isEmpty() }
            (ctor0?.call() ?: objenesis.newInstance(kClass.java))
        }

        entity.init()

        for (prop in kClass.memberProperties) {
            prop.isAccessible = true
            val v = prop.get(entity)

            if (prop.findAnnotation<Required>() != null) {
                if (v == null) error("Property '${prop.name}' is required")
            }

            if (prop.findAnnotation<NonEmpty>() != null) {
                when (v) {
                    is String -> if (v.isEmpty()) error("Property '${prop.name}' must be non-empty")
                    is Collection<*> -> if (v.isEmpty()) error("Property '${prop.name}' must be non-empty")
                }
            }
            if (prop.findAnnotation<Positive>() != null) {
                when (v) {
                    is Int -> if (v <= 0) error("Property '${prop.name}' must be > 0")
                    is Long -> if (v <= 0L) error("Property '${prop.name}' must be > 0")
                }
            }
        }

        put(id, uuid, entity)
        return entity
    }

    /* ───────── internals ───────── */

    private fun T.deepCopy(): T {
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        return adapter.copy(this)
    }

    /* ───────── overloads ───────── */

    fun put(entity: T) =
        put("default", ShortUUID.generate(), entity)

    fun put(id: String, entity: T) =
        put(id, ShortUUID.generate(), entity)

    fun put(uuid: ShortUUID, entity: T) =
        put("default", uuid, entity)

    fun get(uuid: ShortUUID): T? =
        get("default", uuid)

    fun delete(uuid: ShortUUID) =
        delete("default", uuid)

    fun update(uuid: ShortUUID, mutator: T.() -> Unit): Boolean =
        update("default", uuid, mutator)

    fun upsert(uuid: ShortUUID, init: T.() -> Unit): T =
        upsert("default", uuid, init)
}

/* ───────── extension ───────── */

fun String.isASCII(): Boolean = all { it.code <= 127 }

fun <T : Any> BinPack.encode(kClass: KClass<T>, value: T): ByteBuffer {
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    val size = adapter.estimateSize(value)
    val buffer = BinPackBufferPool.get(size)
    adapter.write(value, buffer)
    buffer.flip()
    return buffer
}

fun <T : Any> BinPack.decode(kClass: KClass<T>, buffer: ByteBuffer): T {
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    return adapter.read(buffer)
}

/* ───────── Annotations ───────── */

@Target(AnnotationTarget.PROPERTY)
annotation class Required

@Target(AnnotationTarget.PROPERTY)
annotation class NonEmpty

@Target(AnnotationTarget.PROPERTY)
annotation class Positive
