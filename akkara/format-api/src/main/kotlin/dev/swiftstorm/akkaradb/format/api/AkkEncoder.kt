package dev.swiftstorm.akkaradb.format.api

interface AkkEncoder {
    fun encode(key: ByteArray, value: ByteArray): ByteArray
}