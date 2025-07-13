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
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible

object AkkDSL {

    /* ───────── Packed (high‑performance) ───────── */

    /**
     * Opens/creates a packed table with CBOR encoding.
     * Requires the <strong>@Serializable</strong> plugin on [T].
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

    /** Wrap an existing AkkaraDB instance as <em>packed</em> table. */
    inline fun <reified T : Any> AkkaraDB.packed(
        format: BinaryFormat = Cbor
    ): PackedTable<T> = PackedTable(this, T::class, serializer(), format)

    /* ───────── Field (string) – kept for compatibility ───────── */

    @Deprecated("Use openPacked() unless you need per‑field keys.")
    inline fun <reified T : Any> openFields(baseDir: Path): FieldTable<T> =
        FieldTable(AkkaraDB.open(baseDir), T::class)

    @Deprecated("Use packed() unless you need per‑field keys.")
    inline fun <reified T : Any> AkkaraDB.fields(): FieldTable<T> =
        FieldTable(this, T::class)
}

/* ======================================================================= */
/*  ⚡  PACKED TABLE – CBOR‑backed, single‑record I/O                       */
/* ======================================================================= */

class PackedTable<T : Any>(
    private val db: AkkaraDB,
    private val kClass: KClass<T>,
    private val serializer: KSerializer<T>,
    private val format: BinaryFormat = Cbor,
) {
    private val ns = kClass.qualifiedName ?: kClass.simpleName!!
    private val seq = AtomicLong(0)

    /* ---------- helpers ---------- */

    private fun keyBuf(id: String): ByteBuffer =
        StandardCharsets.UTF_8.encode("$ns:$id")

    /* ---------- CRUD ---------- */

    fun put(id: String, entity: T) {
        val bytes = format.encodeToByteArray(serializer, entity)
        db.put(Record(keyBuf(id), ByteBuffer.wrap(bytes), seq.incrementAndGet()))
    }

    fun get(id: String): T? = db.get(keyBuf(id))?.let { buf ->
        val arr = ByteArray(buf.remaining()).also { buf.get(it) }
        format.decodeFromByteArray(serializer, arr)
    }

    inline fun update(id: String, mutator: T.() -> Unit): Boolean {
        val obj = get(id) ?: return false
        mutator(obj)
        put(id, obj)
        return true
    }

    fun delete(id: String) {
        db.put(Record(keyBuf(id), ByteBuffer.allocate(0), seq.incrementAndGet()))
    }
}

/* ======================================================================= */
/*  💤  FIELD TABLE – Legacy UTF‑8 per‑field storage (less performant)      */
/* ======================================================================= */

class FieldTable<T : Any>(
    private val db: AkkaraDB,
    val kClass: KClass<T>,
) {
    private val ns = kClass.qualifiedName ?: kClass.simpleName!!
    private val seq = AtomicLong(0)

    /* ---------- CRUD ---------- */

    fun put(id: String, entity: T) = kClass.memberProperties.forEach { prop ->
        val key = keyStr(id, prop.name)
        val value = prop.get(entity)?.toString() ?: "null"
        db.put(Record(key, value, seq.incrementAndGet()))
    }

    inline fun put(id: String, builder: T.() -> Unit) {
        val ctor = kClass.primaryConstructor ?: error("Data class must have primary constructor")
        val instance = ctor.callBy(emptyMap())
        builder(instance)
        put(id, instance)
    }

    fun get(id: String): T? {
        val snapshot = HashMap<String, String>()
        for (prop in kClass.memberProperties) {
            val buf = StandardCharsets.UTF_8.encode(keyStr(id, prop.name))
            val bytes = db.get(buf) ?: return null
            snapshot[prop.name] = StandardCharsets.UTF_8.decode(bytes.duplicate()).toString()
        }
        return hydrate(snapshot)
    }

    inline fun update(id: String, mutator: T.() -> Unit): Boolean {
        val current = get(id) ?: return false
        mutator(current)
        put(id, current)
        return true
    }

    fun delete(id: String) = kClass.memberProperties.forEach { prop ->
        db.put(Record(keyStr(id, prop.name), "", seq.incrementAndGet()))
    }

    /* ---------- helpers ---------- */

    private fun keyStr(id: String, field: String) = "$ns:$id:$field"

    /** Build new instance from <field → rawString>. */
    private fun hydrate(src: Map<String, String>): T {
        val ctor = kClass.primaryConstructor ?: error("Data class must have primary constructor")
        val args = HashMap<kotlin.reflect.KParameter, Any?>()
        for (p in ctor.parameters) src[p.name]?.let { args[p] = it }
        val obj = ctor.callBy(args)
        for (prop in kClass.memberProperties) {
            if (prop !is KMutableProperty1) continue
            src[prop.name]?.let { raw ->
                prop.isAccessible = true
                prop.setter.call(obj, raw)
            }
        }
        return obj
    }
}
