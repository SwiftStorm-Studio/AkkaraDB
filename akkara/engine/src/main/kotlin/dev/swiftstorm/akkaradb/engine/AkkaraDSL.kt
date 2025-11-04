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
import dev.swiftstorm.akkaradb.common.ShortUUID
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
 * AkkaraDSL v3: thin convenience layer over AkkaraDB v3.
 * - Provides builder presets to create AkkaraDB.Options
 * - Adds generic PackedTable<T> helpers for typed CRUD with composite keys
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

class PackedTable<T : Any>(val db: AkkaraDB, internal val kClass: KClass<T>) : Closeable {
    override fun close() {
        db.close()
    }

    // Namespace prefix: "<qualifiedName>:"
    private val nsBuf: ByteBufferL by lazy {
        val name = (kClass.qualifiedName ?: kClass.simpleName!!) + ":"
        ByteBufferL.wrap(StandardCharsets.US_ASCII.encode(name))
    }

    companion object {
        private const val SEP: Char = 0x1F.toChar() // ASCII unit separator
        private val EMPTY: ByteBufferL = ByteBufferL.allocate(0)
    }

    private fun String.isAsciiNoSep(): Boolean = all { it.code in 0..0x7F } && indexOf(SEP) == -1

    /**
     * Builds a contiguous key buffer: [namespace][id ASCII][SEP][uuid].
     *
     * This version avoids java.nio encoders and writes ASCII directly via ByteBufferL.
     * Preconditions:
     * - `id.isAsciiNoSep()` ensures all chars are 0x00..0x7F and no 0x1F.
     */
    internal fun keyBuf(id: String, uuid: ShortUUID): ByteBufferL {
        require(id.isAsciiNoSep()) { "ID must be ASCII and not contain 0x1F: $id" }

        // LE-safe, independent cursors
        val rNs = nsBuf.duplicate().position(0)

        // Prefer a ByteBufferL from ShortUUID; fall back to wrap() if only ByteBuffer is available.
        val uuidBuf: ByteBufferL = uuid.toByteBufferL()

        val outCap = rNs.remaining + id.length + 1 + uuidBuf.remaining
        val out = ByteBufferL.allocate(outCap)

        // [namespace]
        out.put(rNs)

        // [id ASCII]
        out.putAscii(id)

        // [SEP]
        out.i8 = SEP.code

        // [uuid bytes]
        out.put(uuidBuf)

        return out
    }

    internal fun prefixBuf(id: String): ByteBufferL {
        require(id.isAsciiNoSep())
        val rNs = nsBuf.duplicate().position(0)
        val idBuf = StandardCharsets.US_ASCII.encode(id)
        val out = ByteBufferL.allocate(rNs.remaining + idBuf.remaining())
        out.put(rNs).put(ByteBufferL.wrap(idBuf))
        return out
    }

    // CRUD
    fun put(id: String, uuid: ShortUUID, entity: T) {
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        // Allocate from binpack pool with an estimated capacity
        val cap = adapter.estimateSize(entity).coerceAtLeast(32)
        val buf = BinPackBufferPool.get(cap)
        adapter.write(entity, buf)
        // Prepare a read-ready view (position=0, limit=written)
        val encoded = buf.asReadOnlyDuplicate().apply { position(0); limit(buf.position) }
        val key = keyBuf(id, uuid)
        db.put(key, encoded)
        // Return pooled buffer
        BinPackBufferPool.release(buf)
    }

    fun get(id: String, uuid: ShortUUID): T? {
        val key = keyBuf(id, uuid)
        val v = db.get(key) ?: return null
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        return adapter.read(v.duplicate().position(0))
    }

    fun delete(id: String, uuid: ShortUUID) {
        val key = keyBuf(id, uuid)
        db.delete(key)
    }

    // Higher-order helpers
    fun update(id: String, uuid: ShortUUID, mutator: T.() -> Unit): Boolean {
        val old = get(id, uuid) ?: return false
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        val copy = adapter.copy(old).apply(mutator)
        if (copy == old) return false
        put(id, uuid, copy)
        return true
    }

    fun upsert(id: String, uuid: ShortUUID, init: T.() -> Unit): T {
        val entity: T = get(id, uuid) ?: run {
            val ctor0 = kClass.constructors.firstOrNull { it.parameters.isEmpty() }
            (ctor0?.call() ?: throw IllegalStateException("No-arg constructor required for ${kClass.simpleName}"))
        }
        entity.init()

        for (prop in kClass.memberProperties) {
            prop.isAccessible = true
            val v = prop.get(entity)
            if (prop.findAnnotation<Required>() != null) {
                if (v == null) error("Property '${'$'}{prop.name}' is required")
            }
            if (prop.findAnnotation<NonEmpty>() != null) {
                when (v) {
                    is String -> if (v.isEmpty()) error("Property '${'$'}{prop.name}' must be non-empty")
                    is Collection<*> -> if (v.isEmpty()) error("Property '${'$'}{prop.name}' must be non-empty")
                }
            }
            if (prop.findAnnotation<Positive>() != null) {
                when (v) {
                    is Int -> if (v <= 0) error("Property '${'$'}{prop.name}' must be > 0")
                    is Long -> if (v <= 0L) error("Property '${'$'}{prop.name}' must be > 0")
                }
            }
        }
        put(id, uuid, entity)
        return entity
    }

    // Overloads
    fun put(entity: T) = put("default", ShortUUID.generate(), entity)
    fun put(id: String, entity: T) = put(id, ShortUUID.generate(), entity)
    fun put(uuid: ShortUUID, entity: T) = put("default", uuid, entity)
    fun get(uuid: ShortUUID): T? = get("default", uuid)
    fun delete(uuid: ShortUUID) = delete("default", uuid)
    fun update(uuid: ShortUUID, mutator: T.() -> Unit): Boolean = update("default", uuid, mutator)
    fun upsert(uuid: ShortUUID, init: T.() -> Unit): T = upsert("default", uuid, init)
}

// ---- annotations ----

@Target(AnnotationTarget.PROPERTY)
annotation class Required

@Target(AnnotationTarget.PROPERTY)
annotation class NonEmpty

@Target(AnnotationTarget.PROPERTY)
annotation class Positive