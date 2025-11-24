package dev.swiftstorm.akkaradb.engine.query

/**
 * Holds a compiled boolean expression tree for filtering (WHERE clause).
 */
class AkkQuery(
    /** Root predicate expression (must evaluate to boolean). */
    val where: AkkExpr<Boolean>
) {
    override fun toString(): String = "AkkQuery(where=$where)"
}