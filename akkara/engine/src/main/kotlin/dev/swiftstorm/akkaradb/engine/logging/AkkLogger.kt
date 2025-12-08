package dev.swiftstorm.akkaradb.engine.logging

interface AkkLogger {
    fun info(msg: String)
    fun warn(msg: String)
    fun error(msg: String)
    fun debug(msg: String)
}