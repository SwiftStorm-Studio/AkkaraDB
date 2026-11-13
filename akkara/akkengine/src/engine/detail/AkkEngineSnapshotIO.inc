/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineSnapshotIO.inc
// Included inside detail::runtime by AkkEngineSupport.hpp.
    inline void writeU32Le(std::ostream& out, uint32_t value) {
        std::array<uint8_t, 4> bytes{};
        for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = value >> (i * 8); }
        out.write(reinterpret_cast<const char*>(bytes.data()), std::ssize(bytes));
        if (!out) { throw std::runtime_error("AkkEngine: failed to write snapshot staging record"); }
    }

    inline void writeU64Le(std::ostream& out, uint64_t value) {
        std::array<uint8_t, 8> bytes{};
        for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = value >> (i * 8); }
        out.write(reinterpret_cast<const char*>(bytes.data()), std::ssize(bytes));
        if (!out) { throw std::runtime_error("AkkEngine: failed to write snapshot staging record"); }
    }

    template<typename Byte, size_t Extent>
    inline void readExact(std::istream& in, std::span<Byte, Extent> out, const char* what) {
        if (out.empty()) { return; }
        in.read(reinterpret_cast<char*>(out.data()), std::ssize(out));
        if (!in) { throw std::runtime_error(std::string{"AkkEngine: failed to read snapshot staging "} + what); }
    }

    [[nodiscard]] inline uint32_t readU32Le(std::istream& in, const char* what) {
        std::array<uint8_t, 4> bytes{};
        readExact(in, std::span{bytes}, what);
        uint32_t out = 0;
        for (size_t i = 0; i < bytes.size(); ++i) { out |= static_cast<uint32_t>(bytes[i]) << (i * 8); }
        return out;
    }

    [[nodiscard]] inline uint64_t readU64Le(std::istream& in, const char* what) {
        std::array<uint8_t, 8> bytes{};
        readExact(in, std::span{bytes}, what);
        uint64_t out = 0;
        for (size_t i = 0; i < bytes.size(); ++i) { out |= static_cast<uint64_t>(bytes[i]) << (i * 8); }
        return out;
    }

    class Crc32cStream {
        public:
            void update(std::span<const uint8_t> bytes) noexcept { for (const auto byte : bytes) { crc_ = (crc_ >> 8u) ^ table()[(crc_ ^ byte) & 0xffu]; } }

            [[nodiscard]] uint32_t finish() const noexcept { return ~crc_; }

        private:
            static const std::array<uint32_t, 256>& table() noexcept {
                static constexpr std::array<uint32_t, 256> values = [] {
                    std::array<uint32_t, 256> out{};
                    for (uint32_t i = 0; i < out.size(); ++i) {
                        uint32_t crc = i;
                        for (uint32_t bit = 0; bit < 8; ++bit) { crc = (crc >> 1u) ^ (0x82F63B78u & (0u - (crc & 1u))); }
                        out[i] = crc;
                    }
                    return out;
                }();
                return values;
            }

            uint32_t crc_ = 0xFFFFFFFFu;
    };

    inline void updateCrcU32Le(Crc32cStream& crc, uint32_t value) noexcept {
        std::array<uint8_t, 4> bytes{};
        for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = value >> (i * 8); }
        crc.update(bytes);
    }

    inline void updateCrcU64Le(Crc32cStream& crc, uint64_t value) noexcept {
        std::array<uint8_t, 8> bytes{};
        for (size_t i = 0; i < bytes.size(); ++i) { bytes[i] = value >> (i * 8); }
        crc.update(bytes);
    }

    struct SnapshotStagingEntryHeader {
        std::vector<uint8_t> key;
        uint64_t valueSize = 0;
        uint32_t valueCrc32c = 0;
        Crc32cStream recordCrc;
    };

    [[nodiscard]] inline SnapshotStagingEntryHeader readSnapshotStagingEntryHeader(std::istream& in) {
        const uint32_t keyLen = readU32Le(in, "entry key length");
        const uint64_t valueLen = readU64Le(in, "entry value length");
        const uint32_t valueCrc32c = readU32Le(in, "entry value crc");
        SnapshotStagingEntryHeader header;
        header.key.resize(keyLen);
        readExact(in, std::span{header.key}, "entry key");
        header.valueSize = valueLen;
        header.valueCrc32c = valueCrc32c;
        updateCrcU32Le(header.recordCrc, keyLen);
        updateCrcU64Le(header.recordCrc, valueLen);
        updateCrcU32Le(header.recordCrc, valueCrc32c);
        header.recordCrc.update(header.key);
        return header;
    }

    class ClusterSnapshotExportFile : public std::enable_shared_from_this<ClusterSnapshotExportFile> {
        public:
            static constexpr std::array<char, 5> MAGIC{'A', 'K', 'S', 'E', '1'};
            static constexpr uint64_t FOLLOW_PUBLISH_BYTES = 1024ull * 1024ull;
            static constexpr size_t REPLAY_CHUNK_BYTES = 1024u * 1024u;
            using EntryVisitor = cluster::ClusterSnapshot::EntryVisitor;
            using Producer = std::function<bool(const EntryVisitor&)>;

            ClusterSnapshotExportFile(fs::path path, uint64_t seq, Producer producer) : path_{std::move(path)}, seq_{seq}, producer_{std::move(producer)} {}
            ~ClusterSnapshotExportFile() { removeFileIfExists(path_); }

            ClusterSnapshotExportFile(const ClusterSnapshotExportFile&) = delete;
            ClusterSnapshotExportFile& operator=(const ClusterSnapshotExportFile&) = delete;

            [[nodiscard]] uint64_t seq() const noexcept { return seq_; }
            [[nodiscard]] const fs::path& path() const noexcept { return path_; }

            [[nodiscard]] bool reusable() const {
                std::lock_guard lock{mutex_};
                return state_ != State::FAILED;
            }

            bool forEachEntry(const EntryVisitor& visitor) {
                if (!visitor) { return false; }
                Producer producer;
                uint64_t replayCount = 0;
                std::exception_ptr failure;
                auto replayReady = false;
                auto failed = false;
                auto followActiveProducer = false;
                {
                    std::lock_guard lock{mutex_};
                    if (state_ == State::COMPLETE) {
                        replayCount = entryCount_;
                        replayReady = true;
                    }
                    else if (state_ == State::FAILED) {
                        failure = failure_;
                        failed = true;
                    }
                    else if (state_ == State::GENERATING) {
                        ++followers_;
                        followActiveProducer = true;
                    }
                    else {
                        state_ = State::GENERATING;
                        producer = std::move(producer_);
                    }
                }
                if (replayReady) { return replay(visitor, replayCount); }
                if (failure) { std::rethrow_exception(failure); }
                if (failed) { return false; }
                if (followActiveProducer) {
                    try {
                        const bool followed = follow(visitor);
                        removeFollower();
                        return followed;
                    }
                    catch (...) {
                        removeFollower();
                        throw;
                    }
                }
                if (!producer) {
                    failGeneration(nullptr);
                    return false;
                }

                try {
                    uint64_t producedEntries = 0;
                    bool primaryVisitorComplete = true;
                    const bool complete = produce(producer, visitor, producedEntries, primaryVisitorComplete);
                    {
                        std::lock_guard lock{mutex_};
                        entryCount_ = complete ? producedEntries : 0;
                        if (complete) { publishedEntries_ = producedEntries; }
                        state_ = complete ? State::COMPLETE : State::FAILED;
                    }
                    completionCv_.notify_all();
                    return complete && primaryVisitorComplete;
                }
                catch (...) {
                    failGeneration(std::current_exception());
                    throw;
                }
            }

        private:
            enum class State : uint8_t {
                PENDING, GENERATING, COMPLETE, FAILED
            };

            bool readEntry(std::istream& in, const EntryVisitor& visitor) const {
                const auto recordStart = in.tellg();
                auto header = readSnapshotStagingEntryHeader(in);
                const bool wireSizeValid = header.key.size() <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull && header.valueSize <= UINT32_MAX &&
                    header.valueSize <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull - header.key.size();
                const bool fileBacked = visitor.fileEntry && wireSizeValid && 8ull + header.key.size() + header.valueSize > visitor.fileEntryThresholdBytes;
                Crc32cStream payloadCrc;
                if (fileBacked) {
                    updateCrcU32Le(payloadCrc, header.key.size());
                    updateCrcU32Le(payloadCrc, header.valueSize);
                    payloadCrc.update(header.key);
                }
                else if (!visitor.beginEntry(header.key, header.valueSize, header.valueCrc32c)) { return false; }
                std::vector<uint8_t> buffer(REPLAY_CHUNK_BYTES);
                Crc32cStream valueCrc;
                uint64_t offset = 0;
                while (offset < header.valueSize) {
                    const size_t count = std::min<uint64_t>(buffer.size(), header.valueSize - offset);
                    readExact(in, std::span{buffer}.first(count), "export entry value");
                    const auto chunk = std::span<const uint8_t>{buffer}.first(count);
                    valueCrc.update(chunk);
                    header.recordCrc.update(chunk);
                    if (fileBacked) { payloadCrc.update(chunk); }
                    else if (!visitor.appendValueChunk(offset, chunk)) { return false; }
                    offset += count;
                }
                if (!fileBacked && header.valueSize == 0 && !visitor.appendValueChunk(0, {})) { return false; }
                const uint32_t recordCrc = readU32Le(in, "export entry crc");
                if (valueCrc.finish() != header.valueCrc32c || header.recordCrc.finish() != recordCrc) {
                    throw std::runtime_error("AkkEngine: corrupt cluster snapshot export entry");
                }
                if (!fileBacked) { return visitor.finishEntry(); }
                if (recordStart < 0) { throw std::runtime_error("AkkEngine: invalid cluster snapshot export offset"); }
                return visitor.fileEntry(
                    cluster::SnapshotFileEntry{
                        .path = path_,
                        .dataOffset = static_cast<uint64_t>(recordStart) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t),
                        .keySize = static_cast<uint32_t>(header.key.size()),
                        .valueSize = header.valueSize,
                        .payloadCrc32c = payloadCrc.finish(),
                        .storage = shared_from_this(),
                    }
                );
            }

            void readHeader(std::istream& in, std::optional<uint64_t> expectedEntryCount) const {
                std::array<char, MAGIC.size()> magic{};
                readExact(in, std::span{magic}, "export header");
                const uint64_t fileSeq = readU64Le(in, "export seq");
                const uint64_t fileEntryCount = readU64Le(in, "export entry count");
                if (magic != MAGIC || fileSeq != seq_ || (expectedEntryCount && fileEntryCount != *expectedEntryCount)) {
                    throw std::runtime_error("AkkEngine: corrupt cluster snapshot export header");
                }
            }

            bool replay(const EntryVisitor& visitor, uint64_t expectedEntryCount) const {
                std::ifstream in{path_, std::ios::binary};
                if (!in) { throw std::runtime_error("AkkEngine: failed to open cluster snapshot export"); }
                readHeader(in, expectedEntryCount);
                for (uint64_t index = 0; index < expectedEntryCount; ++index) { if (!readEntry(in, visitor)) { return false; } }
                char trailing = 0;
                if (in.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in cluster snapshot export"); }
                return true;
            }

            bool follow(const EntryVisitor& visitor) const {
                std::ifstream in;
                uint64_t consumedEntries = 0;
                while (true) {
                    uint64_t availableEntries = 0;
                    uint64_t completedEntryCount = 0;
                    State state;
                    std::exception_ptr failure;
                    {
                        std::unique_lock lock{mutex_};
                        completionCv_.wait(lock, [&] { return publishedEntries_ > consumedEntries || state_ != State::GENERATING; });
                        availableEntries = publishedEntries_;
                        completedEntryCount = entryCount_;
                        state = state_;
                        failure = failure_;
                    }

                    if (!in.is_open() && (availableEntries != 0 || state == State::COMPLETE)) {
                        in.open(path_, std::ios::binary);
                        if (!in) { throw std::runtime_error("AkkEngine: failed to follow cluster snapshot export"); }
                        readHeader(in, std::nullopt);
                    }
                    while (consumedEntries < availableEntries) {
                        if (!readEntry(in, visitor)) { return false; }
                        ++consumedEntries;
                    }
                    if (state == State::COMPLETE) {
                        if (consumedEntries != completedEntryCount) {
                            throw std::runtime_error("AkkEngine: cluster snapshot follower entry count mismatch");
                        }
                        char trailing = 0;
                        if (in.get(trailing)) { throw std::runtime_error("AkkEngine: trailing bytes in cluster snapshot export"); }
                        return true;
                    }
                    if (state == State::FAILED) {
                        if (failure) { std::rethrow_exception(failure); }
                        return false;
                    }
                }
            }

            bool produce(const Producer& producer, const EntryVisitor& visitor, uint64_t& producedEntries, bool& primaryVisitorComplete) {
                std::ofstream out{path_, std::ios::binary | std::ios::trunc};
                if (!out) { throw std::runtime_error("AkkEngine: failed to create cluster snapshot export"); }
                out.write(MAGIC.data(), MAGIC.size());
                writeU64Le(out, seq_);
                writeU64Le(out, 0);
                uint64_t unpublishedBytes = MAGIC.size() + sizeof(uint64_t) * 2;

                bool entryActive = false;
                bool sawChunk = false;
                uint64_t entryValueSize = 0;
                uint64_t entryValueOffset = 0;
                uint32_t entryValueChecksum = 0;
                uint32_t entryKeySize = 0;
                uint64_t entryDataOffset = 0;
                bool entryFileBacked = false;
                Crc32cStream entryValueCrc;
                Crc32cStream entryRecordCrc;
                Crc32cStream entryPayloadCrc;
                const auto addUnpublished = [&](uint64_t bytes) {
                    unpublishedBytes = bytes > UINT64_MAX - unpublishedBytes ? UINT64_MAX : unpublishedBytes + bytes;
                };
                const auto hasConsumer = [&] {
                    if (primaryVisitorComplete) { return true; }
                    std::lock_guard lock{mutex_};
                    return followers_ != 0;
                };
                const EntryVisitor spoolVisitor{
                    .beginEntry = [&](std::span<const uint8_t> key, uint64_t valueSize, uint32_t valueCrc32c) {
                        if (entryActive) { throw std::runtime_error("AkkEngine: nested cluster snapshot entry"); }
                        if (producedEntries == UINT64_MAX) { throw std::overflow_error("AkkEngine: cluster snapshot has too many entries"); }
                        if (key.size() > UINT32_MAX) { throw std::runtime_error("AkkEngine: cluster snapshot key is too large"); }
                        const uint32_t keySize = key.size();
                        const bool wireSizeValid = key.size() <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull && valueSize <= UINT32_MAX && valueSize
                            <= cluster::ReplFrameHeader::MAX_PAYLOAD_SIZE - 8ull - key.size();
                        const uint64_t wireSize = wireSizeValid ? 8ull + key.size() + valueSize : 0;
                        entryActive = true;
                        sawChunk = false;
                        entryValueSize = valueSize;
                        entryValueOffset = 0;
                        entryValueChecksum = valueCrc32c;
                        entryKeySize = keySize;
                        entryFileBacked = visitor.fileEntry && wireSizeValid && wireSize > visitor.fileEntryThresholdBytes;
                        entryValueCrc = Crc32cStream{};
                        entryRecordCrc = Crc32cStream{};
                        entryPayloadCrc = Crc32cStream{};
                        updateCrcU32Le(entryRecordCrc, keySize);
                        updateCrcU64Le(entryRecordCrc, valueSize);
                        updateCrcU32Le(entryRecordCrc, valueCrc32c);
                        entryRecordCrc.update(key);
                        writeU32Le(out, keySize);
                        writeU64Le(out, valueSize);
                        writeU32Le(out, valueCrc32c);
                        const auto dataOffset = out.tellp();
                        if (dataOffset < 0) { throw std::runtime_error("AkkEngine: invalid cluster snapshot export offset"); }
                        entryDataOffset = dataOffset;
                        if (!key.empty()) { out.write(reinterpret_cast<const char*>(key.data()), std::ssize(key)); }
                        if (!out) { throw std::runtime_error("AkkEngine: failed to write cluster snapshot export header"); }
                        addUnpublished(sizeof(uint32_t) * 2 + sizeof(uint64_t) + key.size());
                        if (entryFileBacked) {
                            updateCrcU32Le(entryPayloadCrc, keySize);
                            updateCrcU32Le(entryPayloadCrc, valueSize);
                            entryPayloadCrc.update(key);
                        }
                        else if (primaryVisitorComplete && !visitor.beginEntry(key, valueSize, valueCrc32c)) { primaryVisitorComplete = false; }
                        return hasConsumer();
                    },
                    .appendValueChunk = [&](uint64_t offset, std::span<const uint8_t> chunk) {
                        if (!entryActive || offset != entryValueOffset || chunk.size() > entryValueSize - entryValueOffset || (chunk.empty() && (
                            entryValueSize != 0 || sawChunk))) { throw std::runtime_error("AkkEngine: invalid cluster snapshot value chunk"); }
                        sawChunk = true;
                        if (!chunk.empty()) { out.write(reinterpret_cast<const char*>(chunk.data()), std::ssize(chunk)); }
                        if (!out) { throw std::runtime_error("AkkEngine: failed to write cluster snapshot value chunk"); }
                        entryValueCrc.update(chunk);
                        entryRecordCrc.update(chunk);
                        if (entryFileBacked) { entryPayloadCrc.update(chunk); }
                        addUnpublished(chunk.size());
                        if (!entryFileBacked && primaryVisitorComplete && !visitor.appendValueChunk(offset, chunk)) { primaryVisitorComplete = false; }
                        entryValueOffset += chunk.size();
                        return hasConsumer();
                    },
                    .finishEntry = [&] {
                        if (!entryActive || !sawChunk || entryValueOffset != entryValueSize || entryValueCrc.finish() != entryValueChecksum) {
                            throw std::runtime_error("AkkEngine: incomplete cluster snapshot entry");
                        }
                        writeU32Le(out, entryRecordCrc.finish());
                        if (!out) { throw std::runtime_error("AkkEngine: failed to finish cluster snapshot export entry"); }
                        entryActive = false;
                        ++producedEntries;
                        addUnpublished(sizeof(uint32_t));
                        if (unpublishedBytes >= FOLLOW_PUBLISH_BYTES) {
                            out.flush();
                            if (!out) { throw std::runtime_error("AkkEngine: failed to publish cluster snapshot export entries"); }
                            publishEntries(producedEntries);
                            unpublishedBytes = 0;
                        }
                        if (entryFileBacked && primaryVisitorComplete) {
                            out.flush();
                            if (!out) { throw std::runtime_error("AkkEngine: failed to publish cluster snapshot export entry"); }
                            if (!visitor.fileEntry(
                                cluster::SnapshotFileEntry{
                                    .path = path_,
                                    .dataOffset = entryDataOffset,
                                    .keySize = entryKeySize,
                                    .valueSize = entryValueSize,
                                    .payloadCrc32c = entryPayloadCrc.finish(),
                                    .storage = shared_from_this(),
                                }
                            )) { primaryVisitorComplete = false; }
                        }
                        else if (!entryFileBacked && primaryVisitorComplete && !visitor.finishEntry()) { primaryVisitorComplete = false; }
                        return hasConsumer();
                    },
                    .fileEntryThresholdBytes = UINT64_MAX,
                    .fileEntry = {},
                };
                const bool complete = producer(spoolVisitor);
                if (complete && entryActive) { throw std::runtime_error("AkkEngine: unterminated cluster snapshot entry"); }
                if (!complete) { return false; }
                out.seekp(MAGIC.size() + sizeof(uint64_t));
                writeU64Le(out, producedEntries);
                out.flush();
                if (!out) { throw std::runtime_error("AkkEngine: failed to finish cluster snapshot export"); }
                out.close();
                publishEntries(producedEntries);
                return true;
            }

            void publishEntries(uint64_t count) {
                {
                    std::lock_guard lock{mutex_};
                    publishedEntries_ = count;
                }
                completionCv_.notify_all();
            }

            void removeFollower() {
                std::lock_guard lock{mutex_};
                if (followers_ != 0) { --followers_; }
            }

            void failGeneration(const std::exception_ptr& failure) {
                {
                    std::lock_guard lock{mutex_};
                    failure_ = failure;
                    state_ = State::FAILED;
                }
                completionCv_.notify_all();
            }

            fs::path path_;
            uint64_t seq_;
            mutable std::mutex mutex_;
            mutable std::condition_variable completionCv_;
            State state_ = State::PENDING;
            uint64_t entryCount_ = 0;
            uint64_t publishedEntries_ = 0;
            size_t followers_ = 0;
            Producer producer_;
            std::exception_ptr failure_;
    };

    [[nodiscard]] inline std::vector<uint8_t> encodeSnapshotCommitValue(uint64_t recordCount) {
        std::vector<uint8_t> out;
        out.insert(out.end(), {'A', 'K', 'S', 'C', '1'});
        for (size_t i = 0; i < sizeof(uint64_t); ++i) { out.push_back(recordCount >> (i * 8)); }
        return out;
    }
