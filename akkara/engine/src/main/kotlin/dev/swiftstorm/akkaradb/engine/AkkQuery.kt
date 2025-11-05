package dev.swiftstorm.akkaradb.engine

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

/** Set / variadic node (e.g., IN (v1, v2, v3)). */
data class AkkList<T>(val values: List<T>) : AkkExpr<List<T>>

/** Supported operators (keep minimal first; extend as needed). */
enum class AkkOp {
    // comparisons
    GT, GE, LT, LE, EQ, NEQ,

    // boolean
    AND, OR, NOT,

    // arithmetic (optional baseline)
    ADD, SUB, MUL, DIV,

    // sql-like helpers
    LIKE, IN, IS_NULL, IS_NOT_NULL
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

    /** Render to a simplistic SQL WHERE string with '?' placeholders. */
    fun toSql(): String = where.renderSql()

    /** Extract bound values in left-to-right order. */
    fun bindings(): List<Any?> = mutableListOf<Any?>().also { where.collectBindings(it) }

    override fun toString(): String = "AkkQuery(sql=${toSql()}, bindings=${bindings()})"
}

private fun AkkExpr<*>.renderSql(): String = when (this) {
    is AkkCol<*> -> (table?.let { "$it." } ?: "") + name
    is AkkLit<*> -> "?"
    is AkkList<*> -> values.joinToString(", ") { "?" }.let { "($it)" }
    is AkkBin<*> -> when (op) {
        AkkOp.GT -> "(${lhs.renderSql()} > ${rhs.renderSql()})"
        AkkOp.GE -> "(${lhs.renderSql()} >= ${rhs.renderSql()})"
        AkkOp.LT -> "(${lhs.renderSql()} < ${rhs.renderSql()})"
        AkkOp.LE -> "(${lhs.renderSql()} <= ${rhs.renderSql()})"
        AkkOp.EQ -> "(${lhs.renderSql()} = ${rhs.renderSql()})"
        AkkOp.NEQ -> "(${lhs.renderSql()} <> ${rhs.renderSql()})"
        AkkOp.AND -> "(${lhs.renderSql()} AND ${rhs.renderSql()})"
        AkkOp.OR -> "(${lhs.renderSql()} OR ${rhs.renderSql()})"
        AkkOp.ADD -> "(${lhs.renderSql()} + ${rhs.renderSql()})"
        AkkOp.SUB -> "(${lhs.renderSql()} - ${rhs.renderSql()})"
        AkkOp.MUL -> "(${lhs.renderSql()} * ${rhs.renderSql()})"
        AkkOp.DIV -> "(${lhs.renderSql()} / ${rhs.renderSql()})"
        AkkOp.LIKE -> "(${lhs.renderSql()} LIKE ${rhs.renderSql()})"
        AkkOp.IN -> "(${lhs.renderSql()} IN ${rhs.renderSql()})"
        else -> error("Unsupported binary op in renderer: $op")
    }

    is AkkUn<*> -> when (op) {
        AkkOp.NOT -> "(NOT ${x.renderSql()})"
        AkkOp.IS_NULL -> "(${x.renderSql()} IS NULL)"
        AkkOp.IS_NOT_NULL -> "(${x.renderSql()} IS NOT NULL)"
        else -> error("Unsupported unary op in renderer: $op")
    }

    else -> error("Unknown expression node: ${this::class.simpleName}")
}

private fun AkkExpr<*>.collectBindings(out: MutableList<Any?>): MutableList<Any?> = when (this) {
    is AkkCol<*> -> out
    is AkkLit<*> -> {
        out += value; out
    }

    is AkkList<*> -> {
        values.forEach { out += it }; out
    }

    is AkkBin<*> -> {
        lhs.collectBindings(out); rhs.collectBindings(out)
    }

    is AkkUn<*> -> {
        x.collectBindings(out)
    }

    else -> out
}


/** Create a column node (optional sugar for prototypes/tests). */
fun <T> col(name: String, table: String? = null): AkkCol<T> = AkkCol(table, name)

/** LIKE sugar until operator-rewrite is available. */
infix fun AkkExpr<String>.like(pattern: String): AkkExpr<Boolean> =
    AkkBin(AkkOp.LIKE, this, AkkLit(pattern))

/** IN sugar until operator-rewrite is available. */
infix fun <T> AkkExpr<T>.isin(values: List<T>): AkkExpr<Boolean> =
    AkkBin(AkkOp.IN, this, AkkList(values))

/** IS NULL / IS NOT NULL sugar. */
fun <T> isNull(e: AkkExpr<T?>): AkkExpr<Boolean> = AkkUn(AkkOp.IS_NULL, e)
fun <T> isNotNull(e: AkkExpr<T?>): AkkExpr<Boolean> = AkkUn(AkkOp.IS_NOT_NULL, e)

/** Marker to limit plugin/operator rewrites to the query scope. */
@DslMarker
annotation class AkkQueryDsl