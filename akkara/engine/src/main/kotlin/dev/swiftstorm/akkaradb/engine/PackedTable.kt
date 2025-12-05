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

@file:Suppress("unused")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterResolver
import dev.swiftstorm.akkaradb.common.binpack.BinPackBufferPool
import dev.swiftstorm.akkaradb.engine.query.*
import dev.swiftstorm.akkaradb.engine.util.murmur3_128
import java.io.Closeable
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * A class representing a table abstraction that packs entities for storage and retrieval in a database.
 * Provides CRUD operations, query execution, and entity ID management.
 *
 * @param T The type of the entity stored in the table. Must be a non-nullable type.
 * @param ID The type of the identifier used for entities in the table. Must be a non-nullable type.
 * @param db The database instance to which this table is bound.
 * @param kClass The KClass of the entity type [T], used for reflection and metadata purposes.
 * @param idClass The KClass of the identifier type [ID], used for validation and serialization purposes.
 */
class PackedTable<T : Any, ID : Any>(
    val db: AkkaraDB,
    @PublishedApi internal val kClass: KClass<T>,
    @PublishedApi internal val idClass: KClass<ID>
) : Closeable {
    override fun close() {
        db.close()
    }

    private val nsBuf: ByteBufferL by lazy {
        val fqn = kClass.qualifiedName ?: kClass.simpleName!!
        val nameBytes = ByteBufferL.wrap(
            StandardCharsets.UTF_8.encode(fqn)
        )

        val hash = murmur3_128(nameBytes, seed = 0)

        // Use upper 64bit only
        ByteBufferL.allocate(8).apply {
            i64 = hash[0]
        }
    }

    private val idProp: KProperty1<T, *> by lazy {
        val ids = kClass.memberProperties.filter { it.findAnnotation<Id>() != null }

        require(ids.size == 1) {
            "Type ${kClass.simpleName}: Exactly one @Id is required, found ${ids.size}"
        }

        val p = ids.first()
        p.isAccessible = true

        val actualType = p.returnType.classifier as? KClass<*>
            ?: error("Cannot determine @Id type for ${p.name}")

        require(idClass == actualType) {
            "@Id type mismatch: expected ${idClass.simpleName}, but was ${actualType.simpleName}"
        }

        p
    }

    @Suppress("UNCHECKED_CAST")
    private fun extractId(entity: T): ID = idProp.get(entity) as ID

    private fun encodeId(id: ID): ByteBufferL {
        val adapter = AdapterResolver.getAdapterForClass(idClass)
        val cap = adapter.estimateSize(id).coerceAtLeast(8)
        val buf = BinPackBufferPool.get(cap)

        adapter.write(id, buf)

        val ro = buf.asReadOnlyDuplicate().apply {
            position(0)
            limit(buf.position)
        }

        BinPackBufferPool.release(buf)
        return ro
    }

    internal fun keyBuf(id: ID): ByteBufferL {
        val ns = nsBuf.duplicate().position(0)
        val idBytes = encodeId(id)

        val out = ByteBufferL.allocate(ns.remaining + idBytes.remaining)
        out.put(ns)
        out.put(idBytes)
        val result = out.position(0)

        println("[PackedTable.keyBuf] Generated key:")
        println("  Namespace: ${ns.remaining} bytes")
        println("  ID bytes: ${idBytes.remaining} bytes")
        println("  Total key: ${result.remaining} bytes")
        val keyHex = result.duplicate().let { buf ->
            (0 until minOf(16, buf.remaining)).joinToString(" ") {
                "%02X".format(buf.i8 and 0xFF)
            }
        }
        println("  Key hex: [$keyHex...]")

        return result
    }

    // ========== CRUD ==========

    /**
     * Stores or updates the specified entity in the database using the provided ID.
     *
     * This method serializes the given entity into a binary format using a resolved adapter
     * and stores it in the database under the constructed key derived from the given ID.
     * If an entry already exists for the specified ID, it will be overwritten.
     *
     * @param id The unique identifier of type [ID] used to associate the entity within the database.
     * @param entity The entity of type [T] to be serialized and stored in the database.
     */
    fun put(id: ID, entity: T) {
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        val cap = adapter.estimateSize(entity).coerceAtLeast(32)

        val buf = BinPackBufferPool.get(cap)
        adapter.write(entity, buf)

        val encoded = buf.asReadOnlyDuplicate().apply {
            position(0)
            limit(buf.position)
        }

        val key = keyBuf(id)

        println("[PackedTable.put] Writing entity:")
        println("  Entity: $entity")
        println("  Key: ${key.remaining} bytes")
        println("  Value: ${encoded.remaining} bytes")

        val seq = db.put(key, encoded)

        println("  Assigned seq: $seq")

        val readBack = db.get(key.duplicate().position(0))
        if (readBack != null) {
            println("  ✓ Read back successful: ${readBack.remaining} bytes")
        } else {
            println("  ✗ WARNING: Could not read back immediately after put!")
        }

        BinPackBufferPool.release(buf)
    }

    /**
     * Inserts or updates the provided entity in the database. The entity's ID is extracted and used
     * to construct a key for the underlying storage. If an entry with the entity's ID already exists,
     * it will be overwritten.
     *
     * @param entity The entity of type [T] to be stored in the database.
     */
    fun put(entity: T) {
        val id = extractId(entity)
        put(id, entity)
    }

    /**
     * Retrieves an entity from the database based on the specified ID.
     *
     * This function constructs a key using the given ID, retrieves the raw data associated with
     * that key from the database, resolves an adapter for the entity's class type, and then reads
     * and deserializes the entity from the raw data. If the ID does not exist in the database, or
     * the data has been tombstoned, it returns null.
     *
     * @param id The identifier of the entity to retrieve.
     * @return The deserialized entity of type [T], or null if the ID is not found or the data is invalid.
     */
    fun get(id: ID): T? {
        val key = keyBuf(id)
        val raw = db.get(key) ?: return null
        val adapter = AdapterResolver.getAdapterForClass(kClass)
        return adapter.read(raw.duplicate().position(0))
    }

    /**
     * Deletes an entry from the database corresponding to the provided ID.
     *
     * @param id The identifier of the entry to be deleted. This ID is used to construct the key for locating the entry in the database.
     */
    fun delete(id: ID) {
        db.delete(keyBuf(id))
    }

    /**
     * Inserts or updates an entity in the database based on the specified ID.
     *
     * If the entity with the given ID does not already exist, a new instance of the entity is created using
     * a no-argument constructor. The newly created or existing entity is then modified using the provided initialization
     * block and stored in the database.
     *
     * @param id The identifier of the entity to insert or update.
     * @param init A lambda function to initialize or modify the entity of type [T].
     * @return The newly created or updated entity of type [T].
     */
    fun upsert(id: ID, init: T.() -> Unit): T {
        val entity = get(id) ?: run {
            val ctor = kClass.constructors.firstOrNull { it.parameters.isEmpty() }
                ?: error("No-arg constructor required for ${kClass.simpleName}")
            ctor.call()
        }

        entity.init()
        put(id, entity)
        return entity
    }

    // ========== Query ==========

    fun query(
        @AkkQueryDsl block: T.() -> Boolean
    ): AkkQuery {
        error(
            "Akkara IR plugin was not applied. " +
                    "The Boolean-based query() is intended to be rewritten to akkQuery()."
        )
    }

    /**
     * Execute a query by scanning rows whose keys fall in this table's namespace
     * and evaluating the AkkExpr<Boolean> predicate against decoded entities.
     *
     * Uses AkkaraDB.range(start, end) to only touch keys with the `<ns>:` prefix.
     */
    fun runQ(query: AkkQuery): Sequence<T> {
        val adapter = AdapterResolver.getAdapterForClass(kClass)

        @Suppress("UNCHECKED_CAST")
        val props = kClass.memberProperties
            .onEach { it.isAccessible = true }
            .associateBy { it.name }

        fun eq(l: Any?, r: Any?): Boolean {
            if (l == null || r == null) return l == r
            if (l is Number && r is Number) {
                val (a, b) =
                    if (l is Float || l is Double || r is Float || r is Double) {
                        l.toDouble() to r.toDouble()
                    } else {
                        l.toLong() to r.toLong()
                    }
                return a == b
            }
            return l == r
        }

        fun cmp(l: Any?, r: Any?): Int {
            require(l != null && r != null)

            if (l is Number && r is Number) {
                return when {
                    l is BigDecimal || r is BigDecimal -> {
                        val a = (l as? BigDecimal) ?: BigDecimal(l.toString())
                        val b = (r as? BigDecimal) ?: BigDecimal(r.toString())
                        a.compareTo(b)
                    }

                    l is Double || l is Float || r is Double || r is Float -> {
                        l.toDouble().compareTo(r.toDouble())
                    }

                    else -> {
                        l.toLong().compareTo(r.toLong())
                    }
                }
            }

            if (l is Comparable<*> && l::class == r::class) {
                @Suppress("UNCHECKED_CAST")
                return (l as Comparable<Any>).compareTo(r)
            }

            error("Unsupported compare: ${l::class.simpleName} vs ${r::class.simpleName}")
        }

        fun containsOp(element: Any?, container: Any?): Boolean {
            if (container == null) return false
            return when (container) {
                is Set<*> -> container.contains(element)
                is Iterable<*> -> container.any { eq(it, element) }
                is Array<*> -> container.any { eq(it, element) }
                is BooleanArray -> container.any { eq(it, element) }
                is ByteArray -> container.any { eq(it, element) }
                is ShortArray -> container.any { eq(it, element) }
                is IntArray -> container.any { eq(it, element) }
                is LongArray -> container.any { eq(it, element) }
                is FloatArray -> container.any { eq(it, element) }
                is DoubleArray -> container.any { eq(it, element) }
                is CharArray -> container.any { eq(it, element) }
                is Map<*, *> -> container.containsKey(element)
                is String -> when (element) {
                    null -> false
                    is CharSequence -> container.contains(element)
                    is Char -> element in container
                    else -> element.toString() in container
                }
                else -> false
            }
        }

        fun eval(e: AkkExpr<*>, entity: T): Any? =
            when (e) {
                is AkkLit<*> -> e.value

                is AkkCol<*> -> {
                    val prop = props[e.name]
                        ?: error("No property '${e.name}' in ${kClass.simpleName}")
                    prop.get(entity)
                }

                is AkkUn<*> -> {
                    val x = eval(e.x, entity)
                    when (e.op) {
                        AkkOp.NOT -> !(x as Boolean? ?: false)
                        AkkOp.IS_NULL -> (x == null)
                        AkkOp.IS_NOT_NULL -> (x != null)
                        else -> error("Unsupported unary op: ${e.op}")
                    }
                }

                is AkkBin<*> -> {
                    val l = eval(e.lhs, entity)
                    val r = eval(e.rhs, entity)
                    when (e.op) {
                        AkkOp.EQ -> eq(l, r)
                        AkkOp.NEQ -> !eq(l, r)
                        AkkOp.GT -> cmp(l, r) > 0
                        AkkOp.GE -> cmp(l, r) >= 0
                        AkkOp.LT -> cmp(l, r) < 0
                        AkkOp.LE -> cmp(l, r) <= 0
                        AkkOp.AND -> {
                            val lb = l as Boolean? ?: false
                            val rb = r as Boolean? ?: false
                            lb && rb
                        }
                        AkkOp.OR -> {
                            val lb = l as Boolean? ?: false
                            val rb = r as Boolean? ?: false
                            lb || rb
                        }
                        AkkOp.IN ->
                            containsOp(l, r)
                        AkkOp.NOT_IN ->
                            !containsOp(l, r)
                        else -> error("Unsupported binary op: ${e.op}")
                    }
                }
            }

        // Compute [start, end) for this table's namespace: "<qualifiedName>:"
        val (startKey, endKey) = namespaceRange()

        println("[PackedTable.runQ] Debug Info:")
        println("  Table class: ${kClass.qualifiedName}")
        println("  Namespace hash: ${nsBuf.duplicate().position(0).i64}")
        println("  Range start: ${startKey.remaining} bytes, i64=${startKey.duplicate().position(0).i64}")
        println("  Range end: ${endKey.remaining} bytes, i64=${endKey.duplicate().position(0).i64}")

        return sequence {
            for (rec in db.range(startKey, endKey)) {
                val entity = adapter.read(rec.value.duplicate().position(0))
                if (eval(query.where, entity) as Boolean) {
                    yield(entity)
                }
            }
        }
    }

    fun runToList(@AkkQueryDsl block: T.() -> Boolean) =
        runQ(query(block)).toList()

    fun firstOrNull(@AkkQueryDsl block: T.() -> Boolean) =
        runQ(query(block)).firstOrNull()

    fun exists(@AkkQueryDsl block: T.() -> Boolean) =
        runQ(query(block)).any()

    fun count(@AkkQueryDsl block: T.() -> Boolean) =
        runQ(query(block)).count()

    private fun namespaceRange(): Pair<ByteBufferL, ByteBufferL> {
        val hash = nsBuf.duplicate().position(0).i64

        val start = ByteBufferL.allocate(8).apply {
            i64 = hash
        }.position(0)
        val end = ByteBufferL.allocate(8).apply {
            i64 = hash + 1
        }.position(0)

        return start to end
    }
}

/**
 * Annotation used to mark a property as an identifier for an entity or data model.
 *
 * This annotation can be applied only to properties and is retained at runtime.
 * It is typically used in database-related frameworks or serialization mechanisms
 * to specify a primary key or unique identifier for an object.
 *
 * Annotation Target: PROPERTY
 * Annotation Retention: RUNTIME
 */
@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class Id