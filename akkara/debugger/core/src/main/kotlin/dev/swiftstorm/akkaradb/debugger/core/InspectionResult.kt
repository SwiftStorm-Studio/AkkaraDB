package dev.swiftstorm.akkaradb.debugger.core

import java.time.Instant

/**
 * SSTable inspection result
 */
data class SSTReport(
    val filePath: String,
    val fileSize: Long,
    val footer: FooterInfo,
    val index: IndexInfo,
    val bloom: BloomInfo?,
    val blocks: List<BlockInfo>,
    val errors: List<String> = emptyList()
)

data class FooterInfo(
    val magic: String,
    val version: Int,
    val indexOffset: Long,
    val bloomOffset: Long,
    val entryCount: Int,
    val crc32c: Int
)

data class IndexInfo(
    val entryCount: Int,
    val keySize: Int,
    val entries: List<IndexEntry>
)

data class IndexEntry(
    val firstKey32: String, // hex
    val blockOffset: Long
)

data class BloomInfo(
    val byteSize: Int,
    val hashCount: Int,
    val expectedInsertions: Long
)

data class BlockInfo(
    val offset: Long,
    val payloadLen: Int,
    val recordCount: Int,
    val records: List<RecordSummary>,
    val crcValid: Boolean,
    val crcStored: Int,
    val crcComputed: Int
)

data class RecordSummary(
    val seq: Long,
    val keyLen: Int,
    val valueLen: Int,
    val flags: Int,
    val keyPreview: String, // first 32 bytes hex
    val valuePreview: String?, // first 32 bytes hex (null if tombstone)
    val isTombstone: Boolean,
    val keyData: ByteArray?,
    val valueData: ByteArray?
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RecordSummary

        if (seq != other.seq) return false
        if (keyLen != other.keyLen) return false
        if (valueLen != other.valueLen) return false
        if (flags != other.flags) return false
        if (keyPreview != other.keyPreview) return false
        if (valuePreview != other.valuePreview) return false
        if (isTombstone != other.isTombstone) return false
        if (keyData != null) {
            if (other.keyData == null) return false
            if (!keyData.contentEquals(other.keyData)) return false
        } else if (other.keyData != null) return false
        if (valueData != null) {
            if (other.valueData == null) return false
            if (!valueData.contentEquals(other.valueData)) return false
        } else if (other.valueData != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = seq.hashCode()
        result = 31 * result + keyLen
        result = 31 * result + valueLen
        result = 31 * result + flags
        result = 31 * result + keyPreview.hashCode()
        result = 31 * result + (valuePreview?.hashCode() ?: 0)
        result = 31 * result + isTombstone.hashCode()
        result = 31 * result + (keyData?.contentHashCode() ?: 0)
        result = 31 * result + (valueData?.contentHashCode() ?: 0)
        return result
    }
}

/**
 * WAL inspection result
 */
data class WALReport(
    val filePath: String,
    val fileSize: Long,
    val entries: List<WALEntry>,
    val truncatedTail: Boolean,
    val errors: List<String> = emptyList()
)

data class WALEntry(
    val offset: Long,
    val lsn: Long?,
    val payloadLen: Int,
    val crcValid: Boolean,
    val operation: String, // "ADD" or "DELETE"
    val seq: Long,
    val keyLen: Int,
    val valueLen: Int,
    val keyPreview: String,
    val valuePreview: String?
)

/**
 * Stripe inspection result
 */
data class StripeReport(
    val directory: String,
    val config: StripeConfig,
    val lanes: List<LaneInfo>,
    val stripes: List<StripeInfo>,
    val errors: List<String> = emptyList()
)

data class StripeConfig(
    val k: Int,
    val m: Int,
    val blockSize: Int
)

data class LaneInfo(
    val index: Int,
    val type: String, // "DATA" or "PARITY"
    val filePath: String,
    val fileSize: Long
)

data class StripeInfo(
    val stripeIndex: Long,
    val blockCount: Int,
    val reconstructible: Boolean,
    val parityValid: Boolean?
)

/**
 * Manifest inspection result
 */
data class ManifestReport(
    val filePath: String,
    val fileSize: Long,
    val events: List<ManifestEvent>,
    val errors: List<String> = emptyList()
)

data class ManifestEvent(
    val offset: Long,
    val type: String,
    val timestamp: Instant?,
    val details: Map<String, Any>
)