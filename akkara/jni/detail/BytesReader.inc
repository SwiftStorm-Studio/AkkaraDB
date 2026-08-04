/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// jni/detail/BytesReader.inc
class BytesReader {
    public:
        explicit BytesReader(std::span<const uint8_t> bytes) : bytes_(bytes) {}

        [[nodiscard]] bool eof() const noexcept { return pos_ == bytes_.size(); }
        [[nodiscard]] size_t position() const noexcept { return pos_; }

        [[nodiscard]] std::span<const uint8_t> slice(size_t start, size_t len) const {
            if (start > bytes_.size() || len > bytes_.size() - start) {
                throw std::runtime_error("AkkaraDB query payload slice is out of bounds");
            }
            return bytes_.subspan(start, len);
        }

        [[nodiscard]] uint8_t u8() {
            ensure(1);
            return bytes_[pos_++];
        }

        [[nodiscard]] uint16_t u16() {
            ensure(2);
            const uint16_t out = static_cast<uint16_t>(bytes_[pos_]) | static_cast<uint16_t>(static_cast<uint16_t>(bytes_[pos_ + 1]) << 8);
            pos_ += 2;
            return out;
        }

        [[nodiscard]] uint32_t u32() {
            ensure(4);
            const uint32_t out = static_cast<uint32_t>(bytes_[pos_]) | (static_cast<uint32_t>(bytes_[pos_ + 1]) << 8) | (static_cast<
                uint32_t>(bytes_[pos_ + 2]) << 16) | (static_cast<uint32_t>(bytes_[pos_ + 3]) << 24);
            pos_ += 4;
            return out;
        }

        [[nodiscard]] int32_t i32() { return static_cast<int32_t>(u32()); }

        [[nodiscard]] uint64_t u64() {
            const uint64_t lo = u32();
            const uint64_t hi = u32();
            return lo | (hi << 32);
        }

        [[nodiscard]] int64_t i64() { return static_cast<int64_t>(u64()); }

        [[nodiscard]] std::string strU16() {
            const auto len = static_cast<size_t>(u16());
            ensure(len);
            std::string out(reinterpret_cast<const char*>(bytes_.data() + pos_), len);
            pos_ += len;
            return out;
        }

        [[nodiscard]] std::string strI32() {
            const auto len = static_cast<int32_t>(u32());
            if (len < 0) { throw std::runtime_error("Negative string length in AkkaraDB query payload"); }
            ensure(static_cast<size_t>(len));
            std::string out(reinterpret_cast<const char*>(bytes_.data() + pos_), static_cast<size_t>(len));
            pos_ += static_cast<size_t>(len);
            return out;
        }

        void skip(size_t len) {
            ensure(len);
            pos_ += len;
        }

    private:
        friend std::string readUtf8String(BytesReader& in);
        friend std::optional<std::string> readOptionalUtf8String(BytesReader& in);
        friend std::optional<std::vector<uint8_t>> readOptionalBytes(BytesReader& in);

        void ensure(size_t len) const { if (len > bytes_.size() - pos_) { throw std::runtime_error("Truncated AkkaraDB query payload"); } }

        std::span<const uint8_t> bytes_;
        size_t pos_ = 0;
};

[[nodiscard]] std::string readUtf8String(BytesReader& in) {
    const auto len = in.i32();
    if (len < 0) { throw std::runtime_error("Negative string length in AkkaraDB options payload"); }
    if (len == 0) { return {}; }
    in.ensure(static_cast<size_t>(len));
    std::string out(reinterpret_cast<const char*>(in.bytes_.data() + in.pos_), static_cast<size_t>(len));
    in.skip(static_cast<size_t>(len));
    return out;
}

[[nodiscard]] std::optional<std::string> readOptionalUtf8String(BytesReader& in) {
    const auto len = in.i32();
    if (len < 0) { return std::nullopt; }
    if (len == 0) { return std::string{}; }
    in.ensure(static_cast<size_t>(len));
    std::string out(reinterpret_cast<const char*>(in.bytes_.data() + in.pos_), static_cast<size_t>(len));
    in.skip(static_cast<size_t>(len));
    return out;
}

[[nodiscard]] std::optional<std::vector<uint8_t>> readOptionalBytes(BytesReader& in) {
    const auto len = in.i32();
    if (len < 0) { return std::nullopt; }
    std::vector<uint8_t> out(static_cast<size_t>(len));
    if (len == 0) { return out; }
    in.ensure(static_cast<size_t>(len));
    std::memcpy(out.data(), in.bytes_.data() + in.pos_, static_cast<size_t>(len));
    in.skip(static_cast<size_t>(len));
    return out;
}
