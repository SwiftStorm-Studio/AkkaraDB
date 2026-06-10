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

// akkaradb/include/akkaradb/binpack/detail/MemberPtrTraits.hpp
#pragma once

#include <string_view>
#include <type_traits>

namespace akkaradb::binpack::detail {
    template <typename T>
    struct MemberPtrTraits;

    template <typename Class, typename Member>
    struct MemberPtrTraits<Member Class::*> {
        using ClassType = Class;
        using MemberType = std::remove_cv_t<Member>;
    };

    template <auto MPtr>
    using classOf = typename MemberPtrTraits<decltype(MPtr)>::ClassType;

    template <auto MPtr>
    using memberOf = typename MemberPtrTraits<decltype(MPtr)>::MemberType;

    template <auto MPtr>
    [[nodiscard]] inline std::string_view memberName() noexcept {
        #if defined(_MSC_VER)
        const std::string_view sig = __FUNCSIG__;
        const auto ampLt = sig.rfind("<&");
        if (ampLt == std::string_view::npos) { return {}; }
        const auto gt = sig.find('>', ampLt);
        if (gt == std::string_view::npos) { return {}; }
        const auto cc = sig.rfind("::", gt);
        if (cc == std::string_view::npos || cc < ampLt) { return {}; }
        return sig.substr(cc + 2, gt - cc - 2);
        #elif defined(__clang__)
        const std::string_view sig = __PRETTY_FUNCTION__; const auto rb = sig.rfind(']'); if (rb == std::string_view::npos) { return {}; }
        const auto cc = sig.rfind("::", rb); if (cc == std::string_view::npos) { return {}; } return sig.substr(cc + 2, rb - cc - 2);
        #else
        const std::string_view sig = __PRETTY_FUNCTION__; const auto rb = sig.rfind(']'); if (rb == std::string_view::npos) { return {}; }
        const auto rp = sig.rfind(')', rb); if (rp == std::string_view::npos) { return {}; } const auto cc = sig.rfind("::", rp); if (cc ==
            std::string_view::npos) { return {}; } return sig.substr(cc + 2, rp - cc - 2);
        #endif
    }
} // namespace akkaradb::binpack::detail
