/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/tools/akkaradb_vlog_tool.cpp
#include "VLogToolCore.hpp"

int main(int argc, char** argv) {
    return akkaradb::tools::vlogtool::run(argc, argv, std::cout, std::cerr);
}
