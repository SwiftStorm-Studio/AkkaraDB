package dev.swiftstorm.akkaradb.common.logger

/**
 * Interface for logging within the Yacla framework.
 *
 * This allows pluggable logging backends (e.g., SLF4J, simple console output).
 */
interface AkkaraLogger {

    /**
     * Logs an informational message.
     *
     * @param message The message to log.
     */
    fun info(message: String)

    /**
     * Logs a warning message.
     *
     * @param message The message to log.
     */
    fun warn(message: String)

    /**
     * Logs an error message, optionally with a throwable.
     *
     * @param message The message to log.
     * @param throwable Optional exception to include in the log.
     */
    fun error(message: String, throwable: Throwable? = null)

    /**
     * Logs a debug message.
     *
     * @param message The message to log.
     */
    fun debug(message: String)

    /**
     * Logs a trace message, useful for detailed debugging.
     *
     * @param message The message to log.
     */
    fun trace(message: String)
}
