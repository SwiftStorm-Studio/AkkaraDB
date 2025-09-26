@file:Suppress("unused", "NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.Record
import dev.swiftstorm.akkaradb.common.ShortUUID
import dev.swiftstorm.akkaradb.common.internal.binpack.AdapterResolver
import dev.swiftstorm.akkaradb.common.internal.binpack.BinPack
import dev.swiftstorm.akkaradb.common.internal.binpack.BinPackBufferPool
import dev.swiftstorm.akkaradb.format.akk.parity.RSErrorCorrectingParityCoder
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import org.objenesis.ObjenesisStd
import java.io.Closeable
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
    inline fun <reified T : Any> open(
        baseDir: Path,
        configure: AkkDSLCfgBuilder.() -> Unit = {}
    ): PackedTable<T> {
        val cfg = AkkDSLCfgBuilder(baseDir).apply(configure).build()
        return open(cfg)
    }

    inline fun <reified T : Any> open(
        baseDir: Path,
        mode: StartupMode,
        noinline customize: AkkDSLCfgBuilder.() -> Unit = {}
    ): PackedTable<T> = open(AkkaraPresets.of(baseDir, mode, customize))

    inline fun <reified T : Any> open(cfg: AkkDSLCfg): PackedTable<T> =
        PackedTable(
            AkkaraDB.open(cfg),
            T::class
        )

    inline fun <reified T : Any> AkkaraDB.open(): PackedTable<T> =
        PackedTable(
            this,
            T::class
        )
}

data class StripeCfg(
    val k: Int = 4,
    val m: Int = 2,
    val autoFlush: Boolean = true,
    val isFastMode: Boolean = false,
    val fsyncBatchN: Int = 32,
    val fsyncIntervalMicros: Long = 500,
    val parityCoder: ParityCoder = RSErrorCorrectingParityCoder(m),
    val flushThreshold: Long = 128L * 1024 * 1024
)

data class WalCfg(
    val dir: Path,
    val filePrefix: String = "wal",
    val enableLog: Boolean = false,
    val fastMode: Boolean = true,
    val fsyncBatchN: Int? = 64,
    val fsyncIntervalMicros: Long? = 8_000L,
    val queueCap: Int = 16_384,
    val backoffNanos: Long = 500_000
)

data class ManifestCfg(
    val path: Path,
    val isFastMode: Boolean = true,
    val batchMaxEvents: Int = 128,
    val batchMaxMicros: Long = 500,
    val strongSyncIntervalMillis: Long = 1000
)

data class AkkDSLCfg(
    val baseDir: Path,
    val useAutoFsync: Boolean = false,
    val stripe: StripeCfg = StripeCfg(),
    val metaCacheCap: Int = 1024,
    val wal: WalCfg = WalCfg(baseDir.resolve("wal")),
    val manifest: ManifestCfg = ManifestCfg(baseDir.resolve("MANIFEST"))
)

enum class StartupMode { FAST, NORMAL, DURABLE, CUSTOM }

object AkkaraPresets {
    fun of(
        baseDir: Path,
        mode: StartupMode,
        customize: AkkDSLCfgBuilder.() -> Unit = {}
    ): AkkDSLCfg {
        val builder = AkkDSLCfgBuilder(baseDir)
        when (mode) {
            StartupMode.FAST -> builder.configureFast()
            StartupMode.NORMAL -> builder.configureNormal()
            StartupMode.DURABLE -> builder.configureDurable()
            StartupMode.CUSTOM -> {}
        }
        builder.apply(customize)
        return builder.build()
    }
}

private fun AkkDSLCfgBuilder.configureDurable() {
    metaCacheCap = 1024
    stripe {
        k = 4
        m = 2
        autoFlush = true
        fsyncBatchN = 32
        fsyncIntervalMicros = 500
        parityCoder = RSParityCoder(2)
        flushThreshold = 128L * 1024 * 1024
    }
    wal {
        fastMode = false
        enableLog = false
        fsyncBatchN = 1
        fsyncIntervalMicros = 8_000L
        queueCap = 16_384
        backoffNanos = 500_000
    }
    manifest {
        isFastMode = false
        batchMaxEvents = 1
        batchMaxMicros = 0
        strongSyncIntervalMillis = 0
    }
}

private fun AkkDSLCfgBuilder.configureNormal() {
    metaCacheCap = 2048
    stripe {
        k = 4
        m = 2
        autoFlush = true
        fsyncBatchN = 64
        fsyncIntervalMicros = 1_000
        flushThreshold = 256L * 1024 * 1024
    }
    wal {
        fastMode = true
        enableLog = false
        fsyncBatchN = 256
        fsyncIntervalMicros = 12_000L
        queueCap = 32_768
        backoffNanos = 400_000
    }
    manifest {
        isFastMode = true
        batchMaxEvents = 256
        batchMaxMicros = 1_000
        strongSyncIntervalMillis = 500
    }
}

private fun AkkDSLCfgBuilder.configureFast() {
    metaCacheCap = 4096
    useAutoFsync = true
    stripe {
        k = 4
        m = 1
        autoFlush = false
        isFastMode = true
        parityCoder = RSParityCoder(1)
        flushThreshold = 1L * 1024 * 1024 * 1024
    }
    wal {
        fastMode = true
        enableLog = false
        disableFsync()
        queueCap = 1_000_000
        backoffNanos = 50_000
    }
    manifest {
        isFastMode = true
        batchMaxEvents = 1024
        batchMaxMicros = 2_000
        strongSyncIntervalMillis = 1000
    }
}

/* ==================== Builders ==================== */

class StripeCfgBuilder {
    var k: Int = 4
    var m: Int = 2
    var autoFlush: Boolean = true
    var isFastMode: Boolean = false
    var parityCoder: ParityCoder? = null
    var flushThreshold: Long = 128L * 1024 * 1024
    var fsyncBatchN: Int = 64
    var fsyncIntervalMicros: Long = 1_000

    fun build(): StripeCfg {
        require(k >= 1) { "k must be >= 1" }
        require(m >= 1) { "m must be >= 1" }
        require(flushThreshold > 0) { "flushThreshold must be > 0" }
        val pc = parityCoder ?: RSErrorCorrectingParityCoder(m)
        return StripeCfg(k, m, autoFlush, isFastMode, fsyncBatchN, fsyncIntervalMicros, pc, flushThreshold)
    }
}

class WalCfgBuilder(defaultPath: Path) {
    var dir: Path = defaultPath
    var filePrefix: String = "wal"
    var enableLog: Boolean = false
    var fastMode: Boolean = true           // Durable既定に合わせる
    var fsyncBatchN: Int? = 64
    var fsyncIntervalMicros: Long? = 8_000L
    var queueCap: Int = 16_384
    var backoffNanos: Long = 500_000

    fun disableFsync() {
        fsyncBatchN = null
        fsyncIntervalMicros = null
    }

    fun build(): WalCfg {
        require(fsyncBatchN == null || fsyncBatchN!! >= 1) { "fsyncBatchN must be >= 1" }
        require(fsyncIntervalMicros == null || fsyncIntervalMicros!! >= 0) { "fsyncIntervalMicros must be >= 0" }
        require(queueCap >= 1) { "queueCap must be >= 1" }
        require(backoffNanos >= 0) { "backoffNanos must be >= 0" }
        return WalCfg(
            dir = dir,
            filePrefix = filePrefix,
            enableLog = enableLog,
            fastMode = fastMode,
            fsyncBatchN = fsyncBatchN,
            fsyncIntervalMicros = fsyncIntervalMicros,
            queueCap = queueCap,
            backoffNanos = backoffNanos
        )
    }
}

class ManifestCfgBuilder(defaultPath: Path) {
    var path: Path = defaultPath
    var isFastMode: Boolean = true
    var batchMaxEvents: Int = 128
    var batchMaxMicros: Long = 500
    var strongSyncIntervalMillis: Long = 1000

    fun build(): ManifestCfg {
        require(batchMaxEvents >= 1) { "batchMaxEvents must be >= 1" }
        require(batchMaxMicros >= 0) { "batchMaxMicros must be >= 0" }
        require(strongSyncIntervalMillis >= 0) { "strongSyncIntervalMillis must be >= 0" }
        return ManifestCfg(
            path = path,
            isFastMode = isFastMode,
            batchMaxEvents = batchMaxEvents,
            batchMaxMicros = batchMaxMicros,
            strongSyncIntervalMillis = strongSyncIntervalMillis
        )
    }
}

class AkkDSLCfgBuilder(private val baseDir: Path) {
    var metaCacheCap: Int = 1024
    var useAutoFsync: Boolean = false

    private val stripeBuilder = StripeCfgBuilder()
    private val walBuilder = WalCfgBuilder(baseDir.resolve("wal"))
    private val manifestBuilder = ManifestCfgBuilder(baseDir.resolve("MANIFEST"))

    fun stripe(block: StripeCfgBuilder.() -> Unit) {
        stripeBuilder.apply(block)
    }

    fun wal(block: WalCfgBuilder.() -> Unit) {
        walBuilder.apply(block)
    }

    fun manifest(block: ManifestCfgBuilder.() -> Unit) {
        manifestBuilder.apply(block)
    }

    fun build(): AkkDSLCfg {
        val stripe = stripeBuilder.build()
        val wal = walBuilder.build()
        val manifest = manifestBuilder.build()
        return AkkDSLCfg(
            baseDir = baseDir,
            useAutoFsync = useAutoFsync,
            stripe = stripe,
            metaCacheCap = metaCacheCap,
            wal = wal,
            manifest = manifest
        )
    }
}

/**
 * Type-safe wrapper around AkkaraDB that provides CRUD / UPSERT helpers for a single entity type [T].
 */
class PackedTable<T : Any>(
    val db: AkkaraDB,
    internal val kClass: KClass<T>
) : Closeable {

    override fun close() {
        db.close()
    }

    /* ───────── namespace & seq ───────── */

    private val nsBuf: ByteBufferL by lazy {
        ByteBufferL.wrap(
            StandardCharsets.US_ASCII
            .encode("${kClass.qualifiedName ?: kClass.simpleName!!}:")
            .asReadOnlyBuffer()
        )
    }

    /* ───────── constants ───────── */

    private val objenesis by lazy { ObjenesisStd(true) }

    companion object {
        private const val SEP: Char = 0x1F.toChar() // ASCII 0x1F (Unit Separator)
        private val EMPTY: ByteBufferL = ByteBufferL.allocate(0).asReadOnly() // 0B empty buffer
    }

    /* ───────── key helpers ───────── */

    internal fun keyBuf(id: String, uuid: ShortUUID): ByteBufferL {
        require(id.isAsciiNoSep()) { "ID must be ASCII and must not contain 0x1F: $id" }

        val rNs = nsBuf.duplicate().apply { rewind() }
        val idBuf = StandardCharsets.US_ASCII.encode(id)
        val uuidBuf = uuid.toByteBuffer().duplicate().apply { rewind() } // 16B

        val out = ByteBufferL.allocate(rNs.remaining + idBuf.remaining() + 1 + uuidBuf.remaining())
        out.put(rNs).put(idBuf).put(SEP.code.toByte()).put(uuidBuf)
        out.flip()
        return out.asReadOnly() // LITTLE_ENDIAN
    }

    internal fun prefixBuf(id: String): ByteBufferL {
        require(id.isAsciiNoSep())
        val rNs = nsBuf.duplicate().apply { rewind() }
        val idBuf = StandardCharsets.US_ASCII.encode(id)
        val out = ByteBufferL.allocate(rNs.remaining + idBuf.remaining())
        out.put(rNs).put(idBuf)
        out.flip()
        return out.asReadOnly() // LITTLE_ENDIAN
    }

    private fun String.isAsciiNoSep() =
        all { it.code in 0..0x7F } && indexOf(SEP) == -1

    /* ───────── CRUD ───────── */

    fun put(id: String, uuid: ShortUUID, entity: T) {
        val encoded = BinPack.encode(kClass, entity) // LITTLE_ENDIAN
        val key = keyBuf(id, uuid) // read-only, LITTLE_ENDIAN
        val seqNum = db.nextSeq()

        db.put(Record(key, encoded, seqNum))
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

    fun putUnsafeNoWal(id: String, uuid: ShortUUID, entity: T) {
        val encoded = BinPack.encode(kClass, entity)
        val key = keyBuf(id, uuid)
        val seqNum = db.nextSeq()
        db.putUnsafeNoWal(Record(key, encoded, seqNum))
    }

    fun deleteUnsafeNoWal(id: String, uuid: ShortUUID) {
        val seqNum = db.nextSeq()
        val tomb = Record(keyBuf(id, uuid), Companion.EMPTY, seqNum).asTombstone()
        db.putUnsafeNoWal(tomb)
    }

    fun putUnsafeNoWal(uuid: ShortUUID, entity: T) =
        putUnsafeNoWal("default", uuid, entity)

    fun deleteUnsafeNoWal(uuid: ShortUUID) =
        deleteUnsafeNoWal("default", uuid)

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

fun <T : Any> BinPack.encode(kClass: KClass<T>, value: T): ByteBufferL {
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    val size = adapter.estimateSize(value)
    val buffer = BinPackBufferPool.get(size) // LITTLE_ENDIAN
    adapter.write(value, buffer)
    buffer.flip()
    return ByteBufferL.wrap(buffer).asReadOnly()
}

fun <T : Any> BinPack.decode(kClass: KClass<T>, buffer: ByteBufferL): T {
    val adapter = AdapterResolver.getAdapterForClass(kClass)
    val src = buffer.rewind().duplicate().asReadOnlyByteBuffer()
    return adapter.read(src)
}

/* ───────── Annotations ───────── */

@Target(AnnotationTarget.PROPERTY)
annotation class Required

@Target(AnnotationTarget.PROPERTY)
annotation class NonEmpty

@Target(AnnotationTarget.PROPERTY)
annotation class Positive