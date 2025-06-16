package dev.swiftstorm.akkaradb.common

import org.slf4j.Logger
import org.slf4j.LoggerFactory

val logger: Logger by lazy {
    LoggerFactory.getLogger("akkara-db")
}