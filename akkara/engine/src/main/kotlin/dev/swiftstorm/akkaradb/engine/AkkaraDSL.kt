package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.engine.util.ShortUUID
import net.ririfa.binpack.AdapterResolver
import net.ririfa.binpack.BinPack
import net.ririfa.binpack.BinPackBufferPool
import org.objenesis.ObjenesisStd
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import kotlin.reflect.KClass
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
}

/**
 * Type-safe wrapper around AkkaraDB that provides CRUD / UPSERT helpers for a single entity type [T].
 */
class PackedTable<T : Any>(
    val db: AkkaraDB,
    private val kClass: KClass<T>
) {

    /* ───────── namespace & seq ───────── */

    private val nsBuf: ByteBuffer by lazy {
        StandardCharsets.UTF_8.encode("${kClass.qualifiedName ?: kClass.simpleName!!}:").asReadOnlyBuffer()
    }

    /* ───────── constants ───────── */

    private val objenesis by lazy { ObjenesisStd(true) }

    /* ───────── key helpers ───────── */

    private fun keyBuf(id: String, uuid: ShortUUID = ShortUUID.generate()): ByteBuffer {
        if (!id.isASCII()) error("ID must be ASCII: $id")

        val rNsBuf = nsBuf.duplicate().apply { rewind() }
        val uuidBuf = uuid.toByteBuffer()
        val idBuf = StandardCharsets.UTF_8.encode(id)

        val out = ByteBuffer.allocate(
            rNsBuf.remaining() + uuidBuf.remaining() + 1 + idBuf.remaining()
        )
        out.put(rNsBuf)
        out.put(uuidBuf)
        out.put(':'.code.toByte())
        out.put(idBuf)
        out.flip()
        return out.asReadOnlyBuffer()
    }

    /* ───────── CRUD ───────── */

    fun put(entity: T, id: String = "default", uuid: ShortUUID = ShortUUID.generate()) {
        if (!id.isASCII()) throw IllegalArgumentException("ID must be ASCII: $id")

        val encoded = BinPack.encode(kClass, entity)
        val key = keyBuf(id, uuid)
        val seqNum = db.nextSeq()

        try {
            db.put(Record(key, encoded, seqNum))
        } finally {
            BinPackBufferPool.release(encoded)
        }
    }

    fun get(uuid: ShortUUID, id: String): T? {
        val key = keyBuf(id, uuid)
        return db.getV(key)?.let { buf ->
            if (buf.remaining() == 0) null else BinPack.decode(kClass, buf)
        }
    }

    fun delete(uuid: ShortUUID, id: String) {
        val seqNum = db.nextSeq()
        val rec = Record(keyBuf(id, uuid), ByteBuffer.allocate(0), seqNum).asTombstone()
        db.put(rec)
    }


    /* ───────── Higher-order helpers ───────── */

    fun update(uuid: ShortUUID, id: String, mutator: T.() -> Unit): Boolean {
        while (true) {
            val old = get(uuid, id) ?: return false
            val copy = old.deepCopy()
            mutator(copy)
            if (copy == old) return false
            put(copy, id, uuid)
            return true
        }
    }

    fun upsert(uuid: ShortUUID, id: String, init: T.() -> Unit): T {
        val entity: T = get(uuid, id) ?: run {
            val ctor0 = kClass.constructors.firstOrNull { it.parameters.isEmpty() }
            (ctor0?.call() ?: objenesis.newInstance(kClass.java))
        }

        entity.init()

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
                }
            }
        }

        put(entity, id, uuid)
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
    val buffer = BinPackBufferPool.get(size)
    adapter.write(value, buffer)
    buffer.flip()
    return buffer
}

fun <T : Any> BinPack.decode(kClass: KClass<T>, buffer: ByteBuffer): T {
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    return adapter.read(buffer)
}
