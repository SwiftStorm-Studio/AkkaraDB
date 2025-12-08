package dev.swiftstorm.akkaradb.engine.logging.impl

import dev.swiftstorm.akkaradb.engine.logging.AkkLogger
import java.util.logging.Logger

class AkkLoggerImpl(
    private val isDebugEnabled: () -> Boolean = { false }
) : AkkLogger {
    private val logger: Logger = Logger.getLogger("AkkaraDB")

    override fun info(msg: String) {
        logger.info(msg)
    }

    override fun warn(msg: String) {
        logger.warning(msg)
    }

    override fun error(msg: String) {
        logger.severe(msg)
    }

    override fun debug(msg: String) {
        if (isDebugEnabled()) {
            logger.info("[DEBUG] $msg")
        }
    }
}