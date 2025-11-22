/*
 * AkkaraDB
 * Copyright (C) 2025 Swift Storm Studio
 *
 * This file is part of AkkaraDB.
 *
 * AkkaraDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * AkkaraDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with AkkaraDB.  If not, see <https://www.gnu.org/licenses/>.
 */

@file:Suppress("unused", "NOTHING_TO_INLINE")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterResolver
import dev.swiftstorm.akkaradb.common.binpack.BinPackBufferPool
import dev.swiftstorm.akkaradb.common.putAscii
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder
import dev.swiftstorm.akkaradb.format.api.FlushPolicy
import dev.swiftstorm.akkaradb.format.api.ParityCoder
import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * Utility object providing factory methods for working with AkkaraDB in a type-safe and DSL-friendly manner.
 * Offers functionality to open and configure packed tables using a flexible DSL approach.
 */
object AkkDSL {
    // factory helpers
    inline fun <reified T : Any> open(baseDir: Path, configure: AkkDSLCfgBuilder.() -> Unit = {}): PackedTable<T> {
        val cfg = AkkDSLCfgBuilder(baseDir).apply(configure).build()
        return open<T>(cfg)
    }

    inline fun <reified T : Any> open(baseDir: Path, mode: StartupMode, noinline customize: AkkDSLCfgBuilder.() -> Unit = {}): PackedTable<T> =
        open<T>(AkkaraPresets.of(baseDir, mode, customize))

    inline fun <reified T : Any> open(cfg: AkkDSLCfg): PackedTable<T> {
        val opts = AkkaraDB.Options(
            baseDir = cfg.baseDir,
            k = cfg.k,
            m = cfg.m,
            flushPolicy = FlushPolicy(maxBlocks = cfg.flushMaxBlocks, maxMicros = cfg.flushMaxMicros),
            fastMode = cfg.fastMode,
            walGroupN = cfg.walGroupN,
            walGroupMicros = cfg.walGroupMicros,
            parityCoder = cfg.parityCoder,
            durableCas = cfg.durableCas,
        )
        val db = AkkaraDB.open(opts)
        return PackedTable(db, T::class)
    }

    inline fun <reified T : Any> AkkaraDB.open(): PackedTable<T> = PackedTable(this, T::class)
}

// ---- configuration (v3-focused, minimal) ----

data class AkkDSLCfg(
    val baseDir: Path,
    val k: Int = 4,
    val m: Int = 2,
    val flushMaxBlocks: Int = 32,
    val flushMaxMicros: Long = 500,
    val fastMode: Boolean = true,
    val walGroupN: Int = 32,
    val walGroupMicros: Long = 500,
    val parityCoder: ParityCoder? = null,
    val durableCas: Boolean = false,
)

class AkkDSLCfgBuilder(private val baseDir: Path) {
    var k: Int = 4
    var m: Int = 2
    var flushMaxBlocks: Int = 32
    var flushMaxMicros: Long = 500
    var fastMode: Boolean = true
    var walGroupN: Int = 32
    var walGroupMicros: Long = 500
    var parityCoder: ParityCoder? = null
    var durableCas: Boolean = false

    fun build(): AkkDSLCfg {
        require(k >= 1) { "k must be >= 1" }
        require(m >= 0) { "m must be >= 0" }
        require(flushMaxBlocks >= 1) { "flushMaxBlocks must be >= 1" }
        require(flushMaxMicros >= 0) { "flushMaxMicros must be >= 0" }
        require(walGroupN >= 1) { "walGroupN must be >= 1" }
        require(walGroupMicros >= 0) { "walGroupMicros must be >= 0" }
        return AkkDSLCfg(baseDir, k, m, flushMaxBlocks, flushMaxMicros, fastMode, walGroupN, walGroupMicros, parityCoder, durableCas)
    }
}

enum class StartupMode { ULTRA_FAST, FAST, NORMAL, DURABLE, CUSTOM }

object AkkaraPresets {
    fun of(baseDir: Path, mode: StartupMode, customize: AkkDSLCfgBuilder.() -> Unit = {}): AkkDSLCfg {
        val b = AkkDSLCfgBuilder(baseDir)
        when (mode) {
            StartupMode.ULTRA_FAST -> b.configureUltraFast()
            StartupMode.FAST -> b.configureFast()
            StartupMode.NORMAL -> b.configureNormal()
            StartupMode.DURABLE -> b.configureDurable()
            StartupMode.CUSTOM -> {}
        }
        b.apply(customize)
        return b.build()
    }
}

private fun AkkDSLCfgBuilder.configureDurable() {
    k = 4; m = 2
    fastMode = false
    walGroupN = 1
    walGroupMicros = 0
    flushMaxBlocks = 32
    flushMaxMicros = 500
    parityCoder = RSParityCoder(2)
    durableCas = true
}

private fun AkkDSLCfgBuilder.configureNormal() {
    k = 4; m = 2
    fastMode = true
    walGroupN = 64
    walGroupMicros = 1000
    flushMaxBlocks = 64
    flushMaxMicros = 1000
    parityCoder = RSParityCoder(2)
}

private fun AkkDSLCfgBuilder.configureFast() {
    k = 4; m = 1
    fastMode = true
    walGroupN = 256
    walGroupMicros = 12_000
    flushMaxBlocks = 256
    flushMaxMicros = 2_000
    parityCoder = RSParityCoder(1)
}

private fun AkkDSLCfgBuilder.configureUltraFast() {
    k = 4; m = 1
    fastMode = true
    walGroupN = 512
    walGroupMicros = 50_000
    flushMaxBlocks = 512
    flushMaxMicros = 50_000
    parityCoder = RSParityCoder(1)
}

// ---- PackedTable ----

class PackedTable<T : Any>(
    val db: AkkaraDB,
    @PublishedApi
    internal val kClass: KClass<T>
) : Closeable {

    override fun close() {
        db.close()
    }

    // Namespace prefix: "<qualifiedName>:"
    internal val nsBuf: ByteBufferL by lazy {
        val name = (kClass.qualifiedName ?: kClass.simpleName!!) + ":"
        ByteBufferL.wrap(StandardCharsets.US_ASCII.encode(name))
    }

    companion object {
        private const val SEP: Char = 0x1F.toChar() // unused now, preserved for safety
        private val EMPTY: ByteBufferL = ByteBufferL.allocate(0)
    }

    private fun String.isAscii(): Boolean =
        all { it.code in 0..0x7F }

    /**
     * Build key buffer:
     *   [namespace][id ASCII]
     *
     * Preconditions:
     * - id.isAscii()
     */
    internal fun keyBuf(id: String): ByteBufferL {
        require(id.isAscii()) { "ID must be ASCII: $id" }

        val rNs = nsBuf.duplicate().position(0)
        val out = ByteBufferL.allocate(rNs.remaining + id.length)
        out.put(rNs)
        out.putAscii(id)
        return out
    }

    /**
     * Prefix buffer for listing/query operations.
     * Returns: [namespace][id ASCII prefix]
     */
    internal fun prefixBuf(id: String): ByteBufferL {
        require(id.isAscii())
        val rNs = nsBuf.duplicate().position(0)
        val idBuf = StandardCharsets.US_ASCII.encode(id)
        val out = ByteBufferL.allocate(rNs.remaining + idBuf.remaining())
        out.put(rNs).put(ByteBufferL.wrap(idBuf))
        return out
    }

    // ========== CRUD ==========

    fun put(id: String, entity: T) {
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        val cap = adapter.estimateSize(entity).coerceAtLeast(32)

        val buf = BinPackBufferPool.get(cap)
        adapter.write(entity, buf)

        val encoded = buf.asReadOnlyDuplicate().apply {
            position(0)
            limit(buf.position)
        }

        val key = keyBuf(id)
        db.put(key, encoded)

        BinPackBufferPool.release(buf)
    }

    fun get(id: String): T? {
        val key = keyBuf(id)
        val raw = db.get(key) ?: return null
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        return adapter.read(raw.duplicate().position(0))
    }

    fun delete(id: String) {
        val key = keyBuf(id)
        db.delete(key)
    }

    fun update(id: String, mutator: T.() -> Unit): Boolean {
        val old = get(id) ?: return false
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        val copy = adapter.copy(old).apply(mutator)
        if (copy == old) return false
        put(id, copy)
        return true
    }

    fun upsert(id: String, init: T.() -> Unit): T {
        val entity: T = get(id) ?: run {
            val ctor0 = kClass.constructors.firstOrNull { it.parameters.isEmpty() }
            (ctor0?.call() ?: error("No-arg constructor required for ${kClass.simpleName}"))
        }

        entity.init()

        // Validation
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

        put(id, entity)
        return entity
    }

    // Overloads using "default" id
    fun put(entity: T) = put("default", entity)
    fun get(): T? = get("default")
    fun delete() = delete("default")
    fun update(mutator: T.() -> Unit): Boolean = update("default", mutator)
    fun upsert(init: T.() -> Unit): T = upsert("default", init)

    // Query
    inline fun query(example: T, @AkkQueryDsl block: T.() -> AkkExpr<Boolean>): AkkQuery =
        AkkQuery(block(example))

}

// ---- annotations ----

@Target(AnnotationTarget.PROPERTY)
annotation class Required

@Target(AnnotationTarget.PROPERTY)
annotation class NonEmpty

@Target(AnnotationTarget.PROPERTY)
annotation class Positive