/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTCodec.hpp
#pragma once

[[nodiscard]] static uint32_t readLe32(const uint8_t* src) noexcept {
    return static_cast<uint32_t>(src[0]) | (static_cast<uint32_t>(src[1]) << 8) | (static_cast<uint32_t>(src[2]) << 16) | (static_cast<
        uint32_t>(src[3]) << 24);
}

[[nodiscard]] static uint32_t readBe32(const uint8_t* src) noexcept {
    return (static_cast<uint32_t>(src[0]) << 24) | (static_cast<uint32_t>(src[1]) << 16) | (static_cast<uint32_t>(src[2]) << 8) |
        static_cast<uint32_t>(src[3]);
}

template <typename Field, typename Out>
static void encodeIndexFieldValue(const Field& value, Out& out) {
    using EncodedField = std::remove_cvref_t<Field>;
    if constexpr (isImmutableField<EncodedField>) { encodeIndexFieldValue(value.get(), out); }
    else if constexpr (isRef<EncodedField>) { encodeSortableIntegral(value.rowId(), out); }
    else if constexpr (std::is_integral_v<EncodedField> && !std::is_same_v<EncodedField, bool>) {
        using Unsigned = std::make_unsigned_t<EncodedField>;
        Unsigned sortable = static_cast<Unsigned>(value);
        if constexpr (std::is_signed_v<EncodedField>) { sortable ^= (Unsigned{1} << (sizeof(EncodedField) * 8 - 1)); }
        writeIndexBigEndian(sortable, out);
    }
    else if constexpr (std::is_same_v<EncodedField, float>) {
        uint32_t bits;
        std::memcpy(&bits, &value, sizeof(bits));
        const uint32_t sign = uint32_t{1} << 31;
        bits = (bits & sign) != 0 ? ~bits : bits ^ sign;
        writeIndexBigEndian(bits, out);
    }
    else if constexpr (std::is_same_v<EncodedField, double>) {
        uint64_t bits;
        std::memcpy(&bits, &value, sizeof(bits));
        const uint64_t sign = uint64_t{1} << 63;
        bits = (bits & sign) != 0 ? ~bits : bits ^ sign;
        writeIndexBigEndian(bits, out);
    }
    else { binpack::BinPack::encodeInto(value, out); }
}

template <typename UInt, typename Out>
static void writeIndexBigEndian(UInt value, Out& out) {
    static_assert(std::is_unsigned_v<UInt>);
    for (size_t i = sizeof(UInt); i > 0; --i) { out.push_back(static_cast<uint8_t>(value >> ((i - 1) * 8))); }
}

void makePkKey(const PK& pk, ArenaByteBuffer& out) const {
    out.clear();
    out.insert(out.end(), pkPrefix_.begin(), pkPrefix_.end());
    encodePrimaryKeyBytes(pk, out);
}

template <typename Key>
static void encodePrimaryKeyBytes(const Key& pk, ArenaByteBuffer& out) {
    if constexpr (std::is_integral_v<Key> && !std::is_same_v<Key, bool> && sizeof(Key) <= 8) { encodeSortableIntegral(pk, out); }
    else { binpack::BinPack::encodeInto(pk, out); }
}

[[nodiscard]] bool getByPkBytes(std::span<const uint8_t> pkBytes, Entry& out) const {
    pkKeyBuffer_.clear();
    pkKeyBuffer_.reserve(8 + pkBytes.size());
    pkKeyBuffer_.insert(pkKeyBuffer_.end(), pkPrefix_.begin(), pkPrefix_.end());
    pkKeyBuffer_.insert(pkKeyBuffer_.end(), pkBytes.begin(), pkBytes.end());

    std::span<const uint8_t> valueSpan;
    if (!engine_->getIntoArena(pkKeyBuffer_, *tempArena_, valueSpan)) { return false; }
    out = Entry{decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(valueSpan)};
    attachRefBindings(out.value);
    sealImmutableFields(out.value);
    return true;
}

[[nodiscard]] static PK decodePrimaryKeyBytes(std::span<const uint8_t> pkBytes) {
    if constexpr (std::is_integral_v<PK> && !std::is_same_v<PK, bool> && sizeof(PK) <= 8) { return decodeSortableIntegral<PK>(pkBytes); }
    else { return binpack::BinPack::decode<PK>(pkBytes); }
}

template <typename Integral, typename Out>
static void encodeSortableIntegral(Integral value, Out& out) {
    using Unsigned = std::make_unsigned_t<Integral>;
    Unsigned sortable = static_cast<Unsigned>(value);
    if constexpr (std::is_signed_v<Integral>) { sortable ^= (Unsigned{1} << (sizeof(Integral) * 8 - 1)); }
    writeIndexBigEndian(sortable, out);
}

template <typename Integral>
[[nodiscard]] static Integral decodeSortableIntegral(std::span<const uint8_t> bytes) {
    if (bytes.size() != sizeof(Integral)) { throw std::invalid_argument("PackedTable: malformed primary key"); }

    using Unsigned = std::make_unsigned_t<Integral>;
    Unsigned sortable = 0;
    for (uint8_t b : bytes) { sortable = static_cast<Unsigned>((sortable << 8) | static_cast<Unsigned>(b)); }
    if constexpr (std::is_signed_v<Integral>) { sortable ^= (Unsigned{1} << (sizeof(Integral) * 8 - 1)); }
    return std::bit_cast<Integral>(sortable);
}

void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    engine_->putHinted(key, value, computeKeyFp64(key), buildMiniKey(key));
}

void removeHinted(std::span<const uint8_t> key) { engine_->removeHinted(key, computeKeyFp64(key), buildMiniKey(key)); }

static void encodeRowIdValue(RowId rowId, ArenaByteBuffer& out) {
    out.clear();
    binpack::BinPack::encodeInto(rowId, out);
}

[[nodiscard]] static RowId decodeRowId(std::span<const uint8_t> bytes) { return binpack::BinPack::decode<RowId>(bytes); }

static void makePrefixStartEnd(const std::array<uint8_t, 8>& prefix, ArenaByteBuffer& start, ArenaByteBuffer& end) {
    start.assign(prefix.begin(), prefix.end());
    end.assign(prefix.begin(), prefix.end());
    if (!detail::incrementLexicographicBytes(end.data(), end.size())) { end.clear(); }
}

static std::array<uint8_t, 8> makeTablePrefix(std::string_view name) {
    std::array<uint8_t, 8> out{};
    detail::writeLe64(detail::fnv1a64(name), out.data());
    return out;
}

static std::array<uint8_t, 8> makeMetaPrefix(std::string_view tableName, std::string_view suffix) {
    std::string input;
    input.reserve(tableName.size() + 6 + suffix.size());
    input.append(tableName);
    input.append(":sys:");
    input.append(suffix);
    std::array<uint8_t, 8> out{};
    detail::writeLe64(detail::fnv1a64(input), out.data());
    return out;
}

static std::array<uint8_t, 8> makeIndexPrefix(std::string_view tableName, std::string_view fieldName) {
    std::string input;
    input.reserve(tableName.size() + 5 + fieldName.size());
    input.append(tableName);
    input.append(":idx:");
    input.append(fieldName);
    std::array<uint8_t, 8> out{};
    detail::writeLe64(detail::fnv1a64(input), out.data());
    return out;
}

static std::array<uint8_t, 8> makePrefixIndexPrefix(std::string_view tableName, std::string_view fieldName) {
    std::string input;
    input.reserve(tableName.size() + 5 + fieldName.size());
    input.append(tableName);
    input.append(":pfx:");
    input.append(fieldName);
    std::array<uint8_t, 8> out{};
    detail::writeLe64(detail::fnv1a64(input), out.data());
    return out;
}

void makePkToRowIdKey(const PK& pk, ArenaByteBuffer& out) const {
    out.clear();
    out.insert(out.end(), pkToRowIdPrefix_.begin(), pkToRowIdPrefix_.end());
    encodePrimaryKeyBytes(pk, out);
}

void makeRowIdToPkKey(RowId rowId, ArenaByteBuffer& out) const {
    out.clear();
    out.insert(out.end(), rowIdToPkPrefix_.begin(), rowIdToPkPrefix_.end());
    encodeSortableIntegral(rowId, out);
}

[[nodiscard]] RowId allocateRowId() {
    indexKeyBuffer_.clear();
    indexKeyBuffer_.insert(indexKeyBuffer_.end(), nextRowIdKey_.begin(), nextRowIdKey_.end());

    std::span<const uint8_t> valueSpan;
    RowId next = 1;
    if (engine_->getIntoArena(indexKeyBuffer_, *tempArena_, valueSpan)) { next = decodeRowId(valueSpan); }

    encodeRowIdValue(next + 1, valueBuffer_);
    putHinted(indexKeyBuffer_, valueBuffer_);
    return next;
}

void writeRowIdMapping(const PK& pk, RowId rowId) {
    makePkToRowIdKey(pk, pkKeyBuffer_);
    encodeRowIdValue(rowId, valueBuffer_);
    putHinted(pkKeyBuffer_, valueBuffer_);

    makeRowIdToPkKey(rowId, pkKeyBuffer_);
    valueBuffer_.clear();
    encodePrimaryKeyBytes(pk, valueBuffer_);
    putHinted(pkKeyBuffer_, valueBuffer_);
}

void rewriteRowIdMapping(const PK& oldPk, const PK& newPk, RowId rowId) {
    makePkToRowIdKey(oldPk, pkKeyBuffer_);
    removeHinted(pkKeyBuffer_);
    writeRowIdMapping(newPk, rowId);
}

void removeRowIdMapping(const PK& pk, RowId rowId) {
    makePkToRowIdKey(pk, pkKeyBuffer_);
    removeHinted(pkKeyBuffer_);
    makeRowIdToPkKey(rowId, pkKeyBuffer_);
    removeHinted(pkKeyBuffer_);
}

template <typename Out>
void makeIndexSearchPrefix(const std::array<uint8_t, 8>& prefix, std::span<const uint8_t> fieldBytes, Out& out) const {
    out.clear();
    out.resize(12 + fieldBytes.size());
    std::memcpy(out.data(), prefix.data(), prefix.size());
    detail::writeLe32(static_cast<uint32_t>(fieldBytes.size()), out.data() + 8);
    if (!fieldBytes.empty()) { std::memcpy(out.data() + 12, fieldBytes.data(), fieldBytes.size()); }
}

template <typename Out>
static void encodePrefixIndexStringSegment(std::string_view value, Out& out, bool terminate) {
    for (const unsigned char ch : value) {
        if (ch == 0) {
            out.push_back(0);
            out.push_back(0xFF);
        }
        else { out.push_back(ch); }
    }
    if (terminate) {
        out.push_back(0);
        out.push_back(0);
    }
}

template <typename Out>
void makePrefixIndexSearchPrefix(const std::array<uint8_t, 8>& prefix, std::span<const uint8_t> escapedPrefixBytes, Out& out) const {
    out.clear();
    out.insert(out.end(), prefix.begin(), prefix.end());
    out.insert(out.end(), escapedPrefixBytes.begin(), escapedPrefixBytes.end());
}

[[nodiscard]] static std::optional<size_t> prefixIndexPkOffset(std::span<const uint8_t> key, size_t fieldOffset) {
    for (size_t i = fieldOffset; i + 1 < key.size();) {
        if (key[i] != 0) {
            ++i;
            continue;
        }
        if (key[i + 1] == 0) { return i + 2; }
        if (key[i + 1] == 0xFF) {
            i += 2;
            continue;
        }
        return std::nullopt;
    }
    return std::nullopt;
}

void makeIndexKey(
    const std::array<uint8_t, 8>& prefix,
    std::span<const uint8_t> fieldBytes,
    std::span<const uint8_t> pkKey,
    ArenaByteBuffer& out
) const {
    if (pkKey.size() < pkPrefix_.size()) { throw std::invalid_argument("PackedTable: malformed primary key"); }
    makeIndexSearchPrefix(prefix, fieldBytes, out);
    out.insert(out.end(), pkKey.begin() + static_cast<std::ptrdiff_t>(pkPrefix_.size()), pkKey.end());
}

void makePrefixIndexKey(
    const std::array<uint8_t, 8>& prefix,
    std::span<const uint8_t> escapedFieldBytes,
    std::span<const uint8_t> pkKey,
    ArenaByteBuffer& out
) const {
    if (pkKey.size() < pkPrefix_.size()) { throw std::invalid_argument("PackedTable: malformed primary key"); }
    out.clear();
    out.insert(out.end(), prefix.begin(), prefix.end());
    out.insert(out.end(), escapedFieldBytes.begin(), escapedFieldBytes.end());
    out.insert(out.end(), pkKey.begin() + static_cast<std::ptrdiff_t>(pkPrefix_.size()), pkKey.end());
}

void writeIndexEntries(const Entity& entity, std::span<const uint8_t> pkKey) {
    constexpr std::span<const uint8_t> emptyValue{};
    for (const auto& idx : indexes_) {
        idx.encodeField(entity, fieldBuffer_);
        makeIndexKey(idx.prefix, fieldBuffer_, pkKey, indexKeyBuffer_);
        putHinted(indexKeyBuffer_, emptyValue);
    }
}

void writePrefixIndexEntries(const Entity& entity, std::span<const uint8_t> pkKey) {
    constexpr std::span<const uint8_t> emptyValue{};
    for (const auto& idx : prefixIndexes_) {
        idx.encodeField(entity, fieldBuffer_);
        makePrefixIndexKey(idx.prefix, fieldBuffer_, pkKey, indexKeyBuffer_);
        putHinted(indexKeyBuffer_, emptyValue);
    }
}

void removeIndexEntries(const Entity& entity, std::span<const uint8_t> pkKey) {
    for (const auto& idx : indexes_) {
        idx.encodeField(entity, fieldBuffer_);
        makeIndexKey(idx.prefix, fieldBuffer_, pkKey, indexKeyBuffer_);
        removeHinted(indexKeyBuffer_);
    }
}

void removePrefixIndexEntries(const Entity& entity, std::span<const uint8_t> pkKey) {
    for (const auto& idx : prefixIndexes_) {
        idx.encodeField(entity, fieldBuffer_);
        makePrefixIndexKey(idx.prefix, fieldBuffer_, pkKey, indexKeyBuffer_);
        removeHinted(indexKeyBuffer_);
    }
}
