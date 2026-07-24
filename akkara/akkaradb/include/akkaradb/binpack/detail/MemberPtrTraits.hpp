/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
        const std::string_view sig = __FUNCSIG__; auto end = std::string_view::npos; auto cc = std::string_view::npos; const auto ampLt =
            sig.rfind("<&"); if (ampLt != std::string_view::npos) {
            end = sig.find('>', ampLt);
            if (end == std::string_view::npos) { return {}; }
            cc = sig.rfind("::", end);
            if (cc == std::string_view::npos || cc < ampLt) { return {}; }
            return sig.substr(cc + 2, end - cc - 2);
        } end = sig.rfind(']'); if (end == std::string_view::npos) { return {}; } cc = sig.rfind("::", end); if (cc ==
            std::string_view::npos) { return {}; } return sig.substr(cc + 2, end - cc - 2);
        #elif defined(__clang__)
        const std::string_view sig = __PRETTY_FUNCTION__; const auto rb = sig.rfind(']'); if (rb == std::string_view::npos) { return {}; }
        const auto cc = sig.rfind("::", rb); if (cc == std::string_view::npos) { return {}; } return sig.substr(cc + 2, rb - cc - 2);
        #else
        const std::string_view sig = __PRETTY_FUNCTION__;
        const auto rb = sig.rfind(']');
        if (rb == std::string_view::npos) { return {}; }
        const auto rp = sig.rfind(')', rb);
        if (rp == std::string_view::npos) { return {}; }
        const auto cc = sig.rfind("::", rp);
        if (cc == std::string_view::npos) { return {}; }
        return sig.substr(cc + 2, rp - cc - 2);
        #endif
    }
} // namespace akkaradb::binpack::detail
