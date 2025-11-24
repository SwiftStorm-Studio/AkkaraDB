package dev.swiftstorm.akkaradb.engine.query

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