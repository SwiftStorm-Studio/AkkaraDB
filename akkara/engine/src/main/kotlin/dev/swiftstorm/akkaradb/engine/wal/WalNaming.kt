package dev.swiftstorm.akkaradb.engine.wal

object WalNaming {
    fun regex(prefix: String): Regex {
        val p = Regex.escape(prefix)
        return Regex("""(?i)^$p[-_]?(\d+)\.(?:akw|wal|log)$""")
    }

    fun fileName(prefix: String, idx: Int): String =
        "%s_%06d.log".format(prefix, idx)

    fun parseIndex(prefix: String, name: String): Int? =
        regex(prefix).matchEntire(name)?.groupValues?.get(1)?.toIntOrNull()
}
