package dev.swiftstorm.akkaradb.common.logger.impl

import dev.swiftstorm.akkaradb.common.logger.AkkaraLogger
import org.slf4j.LoggerFactory

object SLF4JAkkaraLogger : AkkaraLogger {
    private val logger = LoggerFactory.getLogger("Akkara")

    override fun info(message: String) {
        logger.info(message)
    }

    override fun warn(message: String) {
        logger.warn(message)
    }

    override fun error(message: String, throwable: Throwable?) {
        if (throwable != null) {
            logger.error(message, throwable)
        } else {
            logger.error(message)
        }
    }

    override fun trace(message: String) {
        logger.trace(message)
    }

    override fun debug(message: String) {
        logger.debug(message)
    }
}