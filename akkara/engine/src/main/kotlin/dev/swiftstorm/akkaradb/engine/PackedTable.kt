@file:Suppress("unused")

package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterResolver
import dev.swiftstorm.akkaradb.common.binpack.BinPackBufferPool
import dev.swiftstorm.akkaradb.common.putAscii
import dev.swiftstorm.akkaradb.engine.query.*
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

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
            require(l != null && r != null) { "Comparison on null: $l vs $r" }
            if (l is Number && r is Number) {
                val (a, b) =
                    if (l is Float || l is Double || r is Float || r is Double) {
                        l.toDouble() to r.toDouble()
                    } else {
                        l.toLong() to r.toLong()
                    }
                @Suppress("UNCHECKED_CAST")
                return when (a) {
                    is Double -> a.compareTo(b as Double)
                    is Long -> a.compareTo(b as Long)
                    else -> error("Unreachable")
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
                is Iterable<*> ->
                    container.any { eq(it, element) }

                is Array<*> ->
                    container.any { eq(it, element) }

                is BooleanArray ->
                    container.any { eq(it, element) }

                is ByteArray ->
                    container.any { eq(it, element) }

                is ShortArray ->
                    container.any { eq(it, element) }

                is IntArray ->
                    container.any { eq(it, element) }

                is LongArray ->
                    container.any { eq(it, element) }

                is FloatArray ->
                    container.any { eq(it, element) }

                is DoubleArray ->
                    container.any { eq(it, element) }

                is CharArray ->
                    container.any { eq(it, element) }

                is Map<*, *> ->
                    container.keys.any { eq(it, element) }

                is String -> when (element) {
                    null -> false
                    is CharSequence -> container.contains(element)
                    is Char -> container.indexOf(element) >= 0
                    else -> container.contains(element.toString())
                }

                else -> false
            }
        }

        fun eval(e: AkkExpr<*>, entity: T): Any? =
            when (e) {
                is AkkLit<*> -> e.value

                is AkkCol<*> ->
                    props[e.name]?.get(entity)
                        ?: error("No property '${e.name}' in ${kClass.simpleName}")

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

        return sequence {
            for (rec in db.range(startKey, endKey)) {
                val entity = adapter.read(rec.value.duplicate().position(0))
                if (eval(query.where, entity) as Boolean) {
                    yield(entity)
                }
            }
        }
    }

    fun runToList(
        @AkkQueryDsl block: T.() -> Boolean
    ): List<T> =
        runQ(query(block)).toList()

    fun firstOrNull(
        @AkkQueryDsl block: T.() -> Boolean
    ): T? =
        runQ(query(block)).firstOrNull()

    fun exists(
        @AkkQueryDsl block: T.() -> Boolean
    ): Boolean =
        runQ(query(block)).any()

    /**
     * Compute [start, end) for keys belonging to this table.
     *
     * Keys are encoded as:
     *   "<qualifiedName>:" + idAscii
     *
     * So all keys are in the byte range:
     *   [nsBuf, nsBufWithLastByte+1)
     */
    private fun namespaceRange(): Pair<ByteBufferL, ByteBufferL> {
        // Dump nsBuf bytes
        val nsBytes = run {
            val d = nsBuf.duplicate().position(0)
            val arr = ByteArray(d.remaining)
            var i = 0
            while (d.remaining > 0) {
                arr[i++] = d.i8.toByte()
            }
            arr
        }

        val start = ByteBufferL.wrap(ByteBuffer.wrap(nsBytes.clone()))

        val hi = nsBytes.clone()
        val lastIdx = hi.lastIndex
        // ASCII only, so no overflow to worry about in practice (':' -> ';' etc.)
        hi[lastIdx] = (hi[lastIdx] + 1).toByte()
        val end = ByteBufferL.wrap(ByteBuffer.wrap(hi))

        return start to end
    }

}