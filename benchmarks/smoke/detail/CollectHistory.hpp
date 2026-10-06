/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include <akk/core/utils/ArenaGenerator.hpp>
#include <vector>
namespace akk_test {
    template<class T> std::vector<T> collectHistory(akkaradb::core::ArenaGenerator<T> history) {
        std::vector<T> result;
        for (const auto& entry : history) { result.push_back(entry); }
        return result;
    }
}
