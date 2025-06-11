package dev.swiftstorm.akkaradb.common

data class Record(
    val key: ByteArray,
    val value: ByteArray,
    val seqNo: Long = 0
) {
    companion object {
        fun recordOf(
            key: String,
            value: String,
            seqNo: Long = 0
        ): Record {
            return Record(
                key = key.toByteArray(Charsets.UTF_8),
                value = value.toByteArray(Charsets.UTF_8),
                seqNo = seqNo
            )
        }
    }

    inline val sKey  get() = key.toString(Charsets.UTF_8)
    inline val sValue get() = value.toString(Charsets.UTF_8)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record

        if (seqNo != other.seqNo) return false
        if (!key.contentEquals(other.key)) return false
        if (!value.contentEquals(other.value)) return false
        if (sKey != other.sKey) return false
        if (sValue != other.sValue) return false

        return true
    }

    override fun hashCode(): Int {
        var result = seqNo.hashCode()
        result = 31 * result + key.contentHashCode()
        result = 31 * result + value.contentHashCode()
        result = 31 * result + sKey.hashCode()
        result = 31 * result + sValue.hashCode()
        return result
    }
}
