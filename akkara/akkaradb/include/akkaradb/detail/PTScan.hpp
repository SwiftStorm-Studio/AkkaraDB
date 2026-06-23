/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkaradb/include/akkaradb/detail/PTScan.hpp
#pragma once

class ScanRange {
    public:
        ScanRange(ScanRange&&) noexcept = default;
        ScanRange& operator=(ScanRange&&) noexcept = default;
        ScanRange(const ScanRange&) = delete;
        ScanRange& operator=(const ScanRange&) = delete;

        [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

        [[nodiscard]] Entry next() {
            if (!pending_) { throw std::out_of_range("PackedTable::ScanRange: no next entry"); }
            Entry out = std::move(*pending_);
            advance();
            return out;
        }

    private:
        friend class PackedTable;

        ScanRange(const PackedTable* table, std::span<const uint8_t> startKey, std::span<const uint8_t> endKey)
            : table_{table},
              scanArena_{std::make_unique<core::BufferArena>()},
              rows_{table_->engine_->scan(*scanArena_, startKey, endKey)},
              it_{rows_.begin()} { advance(); }

        void advance() {
            pending_.reset();
            while (!(it_ == rows_.end())) {
                const auto& raw = *it_;
                const auto key = raw.key;
                if (key.size() < table_->pkPrefix_.size() || std::memcmp(
                    key.data(),
                    table_->pkPrefix_.data(),
                    table_->pkPrefix_.size()
                ) != 0) { return; }

                std::span<const uint8_t> pkBytes{key.data() + table_->pkPrefix_.size(), key.size() - table_->pkPrefix_.size()};
                Entry entry{table_->decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(raw.value)};
                table_->attachRefBindings(entry.value);
                table_->sealImmutableFields(entry.value);
                pending_ = std::move(entry);
                ++it_;
                return;
            }
        }

        const PackedTable* table_;
        std::unique_ptr<core::BufferArena> scanArena_;
        core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
        core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
        std::optional<Entry> pending_;
};

[[nodiscard]] ScanRange scanAll() const {
    resetTempBuffers();
    makePrefixStartEnd(pkPrefix_, scanStartBuffer_, scanEndBuffer_);
    return ScanRange{this, scanStartBuffer_, scanEndBuffer_};
}

[[nodiscard]] ScanRange scan(const PK& startPk) const {
    resetTempBuffers();
    makePkKey(startPk, scanStartBuffer_);
    scanEndBuffer_.assign(pkPrefix_.begin(), pkPrefix_.end());
    if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
    return ScanRange{this, scanStartBuffer_, scanEndBuffer_};
}

[[nodiscard]] ScanRange scan(const PK& startPk, const PK& endPk) const {
    resetTempBuffers();
    makePkKey(startPk, scanStartBuffer_);
    makePkKey(endPk, scanEndBuffer_);
    return ScanRange{this, scanStartBuffer_, scanEndBuffer_};
}
