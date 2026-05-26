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

// akkengine/src/core/record/KeyFingerprint.cpp
#include "akk/core/record/KeyFingerprint.hpp"

#include <algorithm>
#include <array>
#include <cstring>

namespace akkaradb::core {
    namespace {
        class SipHash24 {
            public:
                explicit SipHash24(uint64_t seed = 0x5AD6DCD676D23C25) noexcept {
                    const uint64_t k0 = seed;
                    const uint64_t k1 = seed ^ 0x9E3779B97F4A7C15ULL;

                    v0_ = 0x736f6d6570736575ULL ^ k0;
                    v1_ = 0x646f72616e646f6dULL ^ k1;
                    v2_ = 0x6c7967656e657261ULL ^ k0;
                    v3_ = 0x7465646279746573ULL ^ k1;
                }

                void update(const uint8_t* data, size_t len) noexcept {
                    const auto* ptr = data;
                    const auto* end = data + len;

                    while (ptr + 8 <= end) {
                        uint64_t m;
                        std::memcpy(&m, ptr, 8);
                        compress(m);
                        ptr += 8;
                    }

                    tail_len_ = static_cast<size_t>(end - ptr);
                    std::memcpy(tail_.data(), ptr, tail_len_);
                }

                [[nodiscard]] uint64_t finalize(size_t total_len) noexcept {
                    uint64_t b = static_cast<uint64_t>(total_len) << 56;

                    for (size_t i = 0; i < tail_len_; ++i) { b |= static_cast<uint64_t>(tail_[i]) << (i * 8); }

                    compress(b);

                    v2_ ^= 0xff;
                    for (int i = 0; i < 4; ++i) { round(); }

                    return v0_ ^ v1_ ^ v2_ ^ v3_;
                }

            private:
                void compress(uint64_t m) noexcept {
                    v3_ ^= m;
                    for (int i = 0; i < 2; ++i) { round(); }
                    v0_ ^= m;
                }

                void round() noexcept {
                    v0_ += v1_;
                    v1_ = rotl(v1_, 13);
                    v1_ ^= v0_;
                    v0_ = rotl(v0_, 32);

                    v2_ += v3_;
                    v3_ = rotl(v3_, 16);
                    v3_ ^= v2_;

                    v0_ += v3_;
                    v3_ = rotl(v3_, 21);
                    v3_ ^= v0_;

                    v2_ += v1_;
                    v1_ = rotl(v1_, 17);
                    v1_ ^= v2_;
                    v2_ = rotl(v2_, 32);
                }

                static constexpr uint64_t rotl(uint64_t x, int b) noexcept { return (x << b) | (x >> (64 - b)); }

                uint64_t v0_, v1_, v2_, v3_;
                std::array<uint8_t, 8> tail_{};
                size_t tail_len_{0};
        };
    } // namespace

    uint64_t compute_key_fp64(const uint8_t* key, size_t key_len) noexcept {
        if (key_len == 0) { return 0ULL; }

        SipHash24 hasher;
        hasher.update(key, key_len);
        return hasher.finalize(key_len);
    }

    uint64_t build_mini_key(const uint8_t* key, size_t key_len) noexcept {
        uint64_t mini = 0;
        const size_t copy_len = std::min<size_t>(key_len, 8);

        for (size_t i = 0; i < copy_len; ++i) { mini |= static_cast<uint64_t>(key[i]) << (i * 8); }

        return mini;
    }
} // namespace akkaradb::core
