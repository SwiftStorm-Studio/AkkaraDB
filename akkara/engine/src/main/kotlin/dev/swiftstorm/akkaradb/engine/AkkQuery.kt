package dev.swiftstorm.akkaradb.engine

import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.AdapterResolver
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

    // arithmetic (optional baseline)
    ADD, SUB, MUL, DIV,

    // sql-like helpers
    IS_NULL, IS_NOT_NULL
}

/**
 * Holds a compiled boolean expression tree for filtering (WHERE clause).
 * Provides simple SQL rendering and bound-parameter extraction.
 *
 * NOTE:
 * - This is a minimal scaffold for early integration.
 * - A future Kotlin compiler plugin can rewrite `> < == && || ! in` etc. into this AST.
 */
class AkkQuery(
    /** Root predicate expression (must evaluate to boolean). */
    val where: AkkExpr<Boolean>
) {
    override fun toString(): String = "AkkQuery(where=$where)"
}

/**
 * Execute a query by scanning all rows in this table's namespace and
 * evaluating the AkkExpr<Boolean> predicate against decoded entities.
 *
 * This is a minimal executor:
 *  - Full scan of in-memory + on-disk view exposed by AkkaraDB.iterator()
 *  - Namespace filter (prefix = "<qualifiedName>:")
 *  - Tombstones are skipped
 *  - Entities are decoded via AdapterResolver and evaluated by a small interpreter
 *
 * Future work:
 *  - Range pushdown (detect id equality / range and use prefix-bound scans)
 *  - SSTable index/bloom seek to reduce IO
 *  - Short-circuit evaluation and expression simplification
 */
fun <T : Any> PackedTable<T>.run(query: AkkQuery): Sequence<T> {
    val adapter = AdapterResolver.getAdapterForClass(kClass)

    @Suppress("UNCHECKED_CAST")
    val props = kClass.memberProperties
        .onEach { it.isAccessible = true }
        .associateBy { it.name }

    fun startsWithNamespace(key: ByteBufferL): Boolean {
        val k = key.duplicate().position(0)
        val n = nsBuf.duplicate().position(0)
        if (k.remaining < n.remaining) return false
        repeat(n.remaining) { if (k.i8 != n.i8) return false }
        return true
    }

    fun eval(e: AkkExpr<*>, entity: T): Any? {
        fun eq(l: Any?, r: Any?): Boolean {
            if (l == null || r == null) return l == r
            if (l is Number && r is Number) {
                val (a, b) = if (l is Float || l is Double || r is Float || r is Double)
                    l.toDouble() to r.toDouble() else l.toLong() to r.toLong()
                return a == b
            }
            return l == r
        }

        fun cmp(l: Any?, r: Any?): Int {
            require(l != null && r != null) { "Comparison on null: $l vs $r" }
            if (l is Number && r is Number) {
                val (a, b) = if (l is Float || l is Double || r is Float || r is Double)
                    l.toDouble() to r.toDouble() else l.toLong() to r.toLong()
                return when (a) {
                    is Double -> a.compareTo(b as Double)
                    is Long -> a.compareTo(b as Long)
                    else -> error("Unreachable")
                }
            }
            @Suppress("UNCHECKED_CAST")
            if (l is Comparable<*> && r::class == l::class) {
                return (l as Comparable<Any>).compareTo(r)
            }
            error("Unsupported compare: ${l::class.simpleName} vs ${r::class.simpleName}")
        }

        return when (e) {
            is AkkLit<*> -> e.value
            is AkkCol<*> -> props[e.name]?.get(entity)
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
                val l = eval(e.lhs, entity);
                val r = eval(e.rhs, entity)
                when (e.op) {
                    AkkOp.EQ -> eq(l, r)
                    AkkOp.NEQ -> !eq(l, r)
                    AkkOp.GT -> cmp(l, r) > 0
                    AkkOp.GE -> cmp(l, r) >= 0
                    AkkOp.LT -> cmp(l, r) < 0
                    AkkOp.LE -> cmp(l, r) <= 0
                    AkkOp.AND -> {
                        val l = eval(e.lhs, entity) as Boolean? ?: false
                        if (!l) false else (eval(e.rhs, entity) as Boolean? ?: false)
                    }

                    AkkOp.OR -> {
                        val l = eval(e.lhs, entity) as Boolean? ?: false
                        if (l) true else (eval(e.rhs, entity) as Boolean? ?: false)
                    }

                    else -> error("Unsupported binary op: ${e.op}")
                }
            }

            else -> error("Unknown expr node: ${e::class.simpleName}")
        }
    }

    return sequence {
        for (rec in db.iterator()) {
            if (rec.tombstone) continue
            if (!startsWithNamespace(rec.key)) continue
            val entity = adapter.read(rec.value.duplicate().position(0))
            if (eval(query.where, entity) as Boolean) yield(entity)
        }
    }
}

fun <T : Any> PackedTable<T>.runToList(query: AkkQuery): List<T> = run(query).toList()
fun <T : Any> PackedTable<T>.firstOrNull(query: AkkQuery): T? = run(query).firstOrNull()
fun <T : Any> PackedTable<T>.exists(query: AkkQuery): Boolean = run(query).any()

/** Create a column node (optional sugar for prototypes/tests). */
fun <T> col(name: String, table: String? = null): AkkCol<T> = AkkCol(table, name)

/** IS NULL / IS NOT NULL sugar. */
fun <T> isNull(e: AkkExpr<T?>): AkkExpr<Boolean> = AkkUn(AkkOp.IS_NULL, e)
fun <T> isNotNull(e: AkkExpr<T?>): AkkExpr<Boolean> = AkkUn(AkkOp.IS_NOT_NULL, e)

/** Marker to limit plugin/operator rewrites to the query scope. */
@DslMarker
annotation class AkkQueryDsl