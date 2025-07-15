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
import kotlin.reflect.full.primaryConstructor

object AkkDSL {

    /* ───────── factory helpers ───────── */

    inline fun <reified T : Any> openPacked(
        baseDir: Path,
        format: BinaryFormat = Cbor,
        seqSeed: Long? = null,
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

class PackedTable<T : Any>(
    private val db: AkkaraDB,
    val kClass: KClass<T>,
    private val serializer: KSerializer<T>,
    private val format: BinaryFormat = Cbor,
    seqSeed: Long? = null,
) {
    /* ───────── namespace & seq ───────── */

    private val nsBytes = "${kClass.qualifiedName ?: kClass.simpleName!!}:".toByteArray(StandardCharsets.UTF_8)

    private val seq = AtomicLong(seqSeed ?: db.bestEffortLastSeq())

    /* ───────── internal constants ───────── */

    private val TOMBSTONE: ByteBuffer = ByteBuffer.allocate(0).asReadOnlyBuffer()

    /* ───────── key helpers ───────── */

    private fun keyBuf(id: String): ByteBuffer {
        val idBytes = id.toByteArray(StandardCharsets.UTF_8)
        val buf = ByteBuffer.allocateDirect(nsBytes.size + idBytes.size)
        buf.put(nsBytes).put(idBytes).flip()
        return buf
    }

    /* ───────── CRUD ───────── */

    fun put(id: String, entity: T) {
        val bytes = format.encodeToByteArray(serializer, entity)
        require(bytes.isNotEmpty()) { "Entity encodes to 0 bytes; disallowed because it conflicts with tombstone." }
        db.put(Record(keyBuf(id), ByteBuffer.wrap(bytes), seq.incrementAndGet()))
    }

    fun get(id: String): T? = db.get(keyBuf(id))?.let { buf ->
        if (buf.remaining() == 0) return null              // tombstone or non‑existent
        val arr = ByteArray(buf.remaining()).also { buf.get(it) }
        format.decodeFromByteArray(serializer, arr)
    }

    fun delete(id: String) {
        db.put(Record(keyBuf(id), TOMBSTONE, seq.incrementAndGet()))
    }

    /* ───────── Higher‑order helpers ───────── */

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

    inline fun build(id: String, builder: T.() -> Unit): T {
        val inst = allocateViaPrimaryCtor() ?: error("${kClass.simpleName} has no default constructor")
        inst.builder()
        put(id, inst)
        return inst
    }

    /* ───────── internals ───────── */

    private fun T.deepCopy(): T =
        format.decodeFromByteArray(serializer, format.encodeToByteArray(serializer, this))

    fun allocateViaPrimaryCtor(): T? {
        val ctor = kClass.primaryConstructor ?: return null
        if (ctor.parameters.any { !it.isOptional && !it.type.isMarkedNullable }) return null
        return ctor.callBy(emptyMap())
    }
}

/* ───────── extension ───────── */

private fun AkkaraDB.bestEffortLastSeq(): Long =
    runCatching { javaClass.getMethod("lastSeq").invoke(this) as? Long ?: 0L }.getOrElse { 0L }
