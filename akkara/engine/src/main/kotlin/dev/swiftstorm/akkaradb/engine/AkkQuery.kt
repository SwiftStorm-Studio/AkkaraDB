package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterResolver
import java.nio.ByteBuffer
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * Algebraic expression node used by the query DSL.
 * A Kotlin compiler plugin can rewrite operators into this tree.
 */
sealed interface AkkExpr<T>

/** Column reference (e.g., users.age). */
data class AkkCol<T>(val table: String?, val name: String) : AkkExpr<T>

/** Literal value node. */
data class AkkLit<T>(val value: T) : AkkExpr<T>

/** Binary operator node (e.g., lhs OP rhs). */
data class AkkBin<T>(
    val op: AkkOp,
    val lhs: AkkExpr<*>,
    val rhs: AkkExpr<*>
) : AkkExpr<T>

/** Unary operator node (e.g., NOT x, IS NULL x). */
data class AkkUn<T>(
    val op: AkkOp,
    val x: AkkExpr<*>
) : AkkExpr<T>

/** Supported operators. */
enum class AkkOp {
    // comparisons
    GT, GE, LT, LE, EQ, NEQ,

    // boolean
    AND, OR, NOT,

    // membership
    IN, NOT_IN,

    // sql-like helpers
    IS_NULL, IS_NOT_NULL
}

/**
 * Holds a compiled boolean expression tree for filtering (WHERE clause).
 */
class AkkQuery(
    /** Root predicate expression (must evaluate to boolean). */
    val where: AkkExpr<Boolean>
) {
    override fun toString(): String = "AkkQuery(where=$where)"
}

/**
 * Execute a query by scanning rows whose keys fall in this table's namespace
 * and evaluating the AkkExpr<Boolean> predicate against decoded entities.
 *
 * Uses AkkaraDB.range(start, end) to only touch keys with the `<ns>:` prefix.
 */
fun <T : Any> PackedTable<T>.run(query: AkkQuery): Sequence<T> {
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

            else -> error("Unknown expr node: ${e::class.simpleName}")
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

// Use AkkaraDB.run(...) helpers
fun <T : Any> PackedTable<T>.runToList(query: AkkQuery): List<T> = run(query).toList()
fun <T : Any> PackedTable<T>.firstOrNull(query: AkkQuery): T? = run(query).firstOrNull()
fun <T : Any> PackedTable<T>.exists(query: AkkQuery): Boolean = run(query).any()

inline fun <T : Any> PackedTable<T>.runToList(
    example: T,
    @AkkQueryDsl block: T.() -> AkkExpr<Boolean>
): List<T> =
    run(query(example, block)).toList()

inline fun <T : Any> PackedTable<T>.firstOrNull(
    example: T,
    @AkkQueryDsl block: T.() -> AkkExpr<Boolean>
): T? =
    run(query(example, block)).firstOrNull()

inline fun <T : Any> PackedTable<T>.exists(
    example: T,
    @AkkQueryDsl block: T.() -> AkkExpr<Boolean>
): Boolean =
    run(query(example, block)).any()

/** Create a column node (optional sugar for prototypes/tests). */
fun <T> col(name: String, table: String? = null): AkkCol<T> = AkkCol(table, name)

/** IS NULL / IS NOT NULL sugar. */
fun <T> isNull(e: AkkExpr<T?>): AkkExpr<Boolean> = AkkUn(AkkOp.IS_NULL, e)
fun <T> isNotNull(e: AkkExpr<T?>): AkkExpr<Boolean> = AkkUn(AkkOp.IS_NOT_NULL, e)

/** Marker to limit plugin/operator rewrites to the query scope. */
@DslMarker
annotation class AkkQueryDsl

// ──────────────────────
// Namespace range helper
// ──────────────────────

/**
 * Compute [start, end) for keys belonging to this table.
 *
 * Keys are encoded as:
 *   "<qualifiedName>:" + idAscii
 *
 * So all keys are in the byte range:
 *   [nsBuf, nsBufWithLastByte+1)
 */
private fun PackedTable<*>.namespaceRange(): Pair<ByteBufferL, ByteBufferL> {
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
