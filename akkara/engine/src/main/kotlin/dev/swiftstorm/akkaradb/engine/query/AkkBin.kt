package dev.swiftstorm.akkaradb.engine.query

/** Binary operator node (e.g., lhs OP rhs). */
data class AkkBin<T>(
    val op: AkkOp,
    val lhs: AkkExpr<*>,
    val rhs: AkkExpr<*>
) : AkkExpr<T>