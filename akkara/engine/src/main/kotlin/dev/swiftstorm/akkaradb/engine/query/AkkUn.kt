package dev.swiftstorm.akkaradb.engine.query

/** Unary operator node (e.g., NOT x, IS NULL x). */
data class AkkUn<T>(
    val op: AkkOp,
    val x: AkkExpr<*>
) : AkkExpr<T>