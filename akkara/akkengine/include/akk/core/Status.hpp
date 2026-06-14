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

// akkengine/include/akk/core/Status.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <cstring>
#include <string_view>
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/utils/StringUtil.hpp"

namespace akkaradb::core {
    /**
     * @brief Lightweight status object used across AkkaraDB core and storage layers.
     *
     * Status represents the outcome of operations in a zero-exception design.
     * It is designed for high-frequency DB paths such as:
     * - MemTable writes
     * - WAL appends
     * - SST flush operations
     *
     * ## Design principles
     *
     * - No heap allocation on success path
     * - Error messages are optional and Arena-backed
     * - Copying Status is cheap (pointer-sized message reference)
     * - No ownership of error message memory
     *
     * ## Memory model
     *
     * - Success (OK): no message pointer used
     * - Error with message:
     *   - message string is allocated inside an Arena
     *   - Status holds a non-owning pointer to Arena memory
     *
     * ## Thread safety
     *
     * Status itself is immutable and thread-safe.
     * Arena is assumed to be externally synchronized or thread-confined.
     */
    class AKDB_API Status {
        public:
            /**
             * @brief Canonical error codes used across AkkaraDB.
             *
             * Keep this set minimal and stable.
             * Do not encode subsystem-specific errors here.
             */
            enum class Code : uint8_t {
                OK = 0, NOT_FOUND, INVALID_ARGUMENT, IO_ERROR, CORRUPTION, OUT_OF_MEMORY, INTERNAL_ERROR
            };

            /**
             * @brief Create success status.
             */
            static Status OK() noexcept { return {Code::OK, nullptr}; }

            /**
             * @brief Create error status without message.
             *
             * @param code Error code.
             */
            static Status Error(Code code) noexcept { return {code, nullptr}; }

            /**
             * @brief Create error status with static message (no allocation).
             *
             * @param code Error code.
             * @param msg  Static string (must outlive Status usage).
             */
            static Status Error(Code code, const char* msg) noexcept { return {code, msg}; }

            /**
             * @brief Create error status with Arena-backed message.
             *
             * @param code  Error code.
             * @param msg   Message to copy into Arena.
             * @param arena Arena used for allocation.
             */
            static Status Error(Code code, std::string_view msg, BufferArena* arena);

            /**
             * @brief Check if status is OK.
             */
            [[nodiscard]] bool ok() const noexcept { return code_ == Code::OK; }

            /**
             * @brief Get error code.
             */
            [[nodiscard]] Code code() const noexcept { return code_; }

            /**
             * @brief Get error message (if any).
             *
             * Returns empty view for OK status or missing message.
             */
            [[nodiscard]] std::string_view message() const noexcept { return msg_ ? std::string_view(msg_) : std::string_view{}; }

            /**
             * @brief Boolean conversion (true = OK).
             */
            explicit operator bool() const noexcept { return ok(); }

        private:
            Code code_;
            const char* msg_; // non-owning pointer (static or Arena-backed)

            constexpr Status(Code code, const char* msg) noexcept : code_(code), msg_(msg) {}
    };

    /**
     * @brief Convert Status to error with Arena-backed message.
     *
     * This function performs allocation inside Arena.
     *
     * @param code  Error code.
     * @param msg   Error message.
     * @param arena Arena allocator.
     * @return Status with Arena-owned message pointer.
     */
    inline Status Status::Error(Code code, std::string_view msg, BufferArena* arena) {
        if (!arena || msg.empty()) { return {code, nullptr}; }

        const char* copied = copyString(msg, arena);
        return {code, copied};
    }
} // namespace akkaradb::core
