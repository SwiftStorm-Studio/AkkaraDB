/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akk/engine/cluster/detail/ClusterConnection.hpp"

namespace akkaradb::engine::cluster::detail {
    class AKKARADB_CLUSTER_RUNTIME_API CancellableSocketReader {
    public:
        CancellableSocketReader();
        ~CancellableSocketReader();
        CancellableSocketReader(const CancellableSocketReader&) = delete;
        CancellableSocketReader& operator=(const CancellableSocketReader&) = delete;
        void cancel();
        // Only reset after the previous connection's read has finished.
        void reset();
        bool cancelled() const;
        bool readAll(ConnectionSocket socket, uint8_t* data, size_t size, ConnectionDeadline deadline);
    private:
        class Impl;
        std::unique_ptr<Impl> impl_;
    };
}
