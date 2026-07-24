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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// akkserver/src/grpc/AkkaraGRPCServer.cpp
#include "akkaradb/grpc/AkkaraGRPCServer.hpp"

#include "akk/engine/server/AkkApiTransportProvider.hpp"

#include "akkaradb_grpc.grpc.pb.h"

#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <fstream>
#include <iterator>
#include <limits>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace akkaradb::grpcapi {
    namespace {
        namespace wire = ::akkaradb::grpcapi::v1;

        [[nodiscard]] std::span<const uint8_t> bytes(const std::string& value) noexcept {
            return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
        }

        [[nodiscard]] std::string readTextFile(const std::filesystem::path& path) {
            std::ifstream in{path, std::ios::binary};
            if (!in) { throw std::runtime_error("AkkaraGRPCServer: failed to open " + path.string()); }
            return {std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
        }

        void setBytes(std::string* out, std::span<const uint8_t> value) {
            if (value.empty()) {
                out->clear();
                return;
            }
            out->assign(reinterpret_cast<const char*>(value.data()), value.size());
        }

        void copyStats(const engine::EngineStats& stats, wire::EngineStats* out) {
            out->set_current_seq(stats.currentSeq);
            out->set_node_id(stats.nodeId);
            out->set_puts_total(stats.putsTotal);
            out->set_removes_total(stats.removesTotal);
            out->set_gets_total(stats.getsTotal);
            out->set_gets_memtable_hit(stats.getsMemtableHit);
            out->set_gets_sst_hit(stats.getsSstHit);
            out->set_gets_miss(stats.getsMiss);
            out->set_exists_total(stats.existsTotal);
            out->set_scans_total(stats.scansTotal);
            out->set_blob_puts_total(stats.blobPutsTotal);

            auto* api = out->mutable_api();
            api->set_enabled(stats.api.enabled);
            api->set_http_enabled(stats.api.httpEnabled);
            api->set_http_tls_enabled(stats.api.httpTlsEnabled);
            api->set_http_port(stats.api.httpPort);
            api->set_http_max_batch_items(stats.api.httpMaxBatchItems);
            api->set_http_max_scan_items(stats.api.httpMaxScanItems);
            api->set_http_max_history_entries(stats.api.httpMaxHistoryEntries);
            api->set_http_max_content_length(stats.api.httpMaxContentLength);
            api->set_http_connections_accepted_total(stats.api.httpConnectionsAcceptedTotal);
            api->set_http_connections_closed_total(stats.api.httpConnectionsClosedTotal);
            api->set_http_connections_active(stats.api.httpConnectionsActive);
            api->set_http_requests_total(stats.api.httpRequestsTotal);
            api->set_http_responses_total(stats.api.httpResponsesTotal);
            api->set_http_bytes_received_total(stats.api.httpBytesReceivedTotal);
            api->set_http_bytes_sent_total(stats.api.httpBytesSentTotal);
            api->set_http_protocol_errors_total(stats.api.httpProtocolErrorsTotal);
            api->set_http_errors_total(stats.api.httpErrorsTotal);
            api->set_http_batch_put_items_total(stats.api.httpBatchPutItemsTotal);
            api->set_http_batch_get_items_total(stats.api.httpBatchGetItemsTotal);
            api->set_tcp_enabled(stats.api.tcpEnabled);
            api->set_tcp_tls_enabled(stats.api.tcpTlsEnabled);
            api->set_tcp_io_backend(stats.api.tcpIoBackend);
            api->set_tcp_worker_threads(stats.api.tcpWorkerThreads);
            api->set_tcp_accept_queue_limit(stats.api.tcpAcceptQueueLimit);
            api->set_tcp_accept_queue_timeout_ms(stats.api.tcpAcceptQueueTimeoutMs);
            api->set_tcp_listen_backlog(stats.api.tcpListenBacklog);
            api->set_tcp_read_timeout_ms(stats.api.tcpReadTimeoutMs);
            api->set_tcp_write_timeout_ms(stats.api.tcpWriteTimeoutMs);
            api->set_tcp_connections_accepted_total(stats.api.tcpConnectionsAcceptedTotal);
            api->set_tcp_connections_closed_total(stats.api.tcpConnectionsClosedTotal);
            api->set_tcp_connections_active(stats.api.tcpConnectionsActive);
            api->set_tcp_accept_queue_depth(stats.api.tcpAcceptQueueDepth);
            api->set_tcp_accept_queue_peak_depth(stats.api.tcpAcceptQueuePeakDepth);
            api->set_tcp_accept_queue_rejected_total(stats.api.tcpAcceptQueueRejectedTotal);
            api->set_tcp_accept_queue_expired_total(stats.api.tcpAcceptQueueExpiredTotal);
            api->set_tcp_requests_total(stats.api.tcpRequestsTotal);
            api->set_tcp_responses_total(stats.api.tcpResponsesTotal);
            api->set_tcp_bytes_received_total(stats.api.tcpBytesReceivedTotal);
            api->set_tcp_bytes_sent_total(stats.api.tcpBytesSentTotal);
            api->set_tcp_protocol_errors_total(stats.api.tcpProtocolErrorsTotal);
            api->set_tcp_crc_errors_total(stats.api.tcpCrcErrorsTotal);
            api->set_tcp_pipeline_batches_total(stats.api.tcpPipelineBatchesTotal);
            api->set_tcp_backpressure_flushes_total(stats.api.tcpBackpressureFlushesTotal);
            api->set_tcp_backpressure_disconnects_total(stats.api.tcpBackpressureDisconnectsTotal);
            api->set_tcp_batch_put_items_total(stats.api.tcpBatchPutItemsTotal);
            api->set_tcp_batch_get_items_total(stats.api.tcpBatchGetItemsTotal);
            api->set_grpc_enabled(stats.api.grpcEnabled);
            api->set_grpc_tls_enabled(stats.api.grpcTlsEnabled);
            api->set_grpc_port(stats.api.grpcPort);
            api->set_grpc_worker_threads(stats.api.grpcWorkerThreads);
            api->set_grpc_completion_queues(stats.api.grpcCompletionQueues);
            api->set_grpc_min_pollers(stats.api.grpcMinPollers);
            api->set_grpc_max_pollers(stats.api.grpcMaxPollers);
            api->set_grpc_max_concurrent_streams(stats.api.grpcMaxConcurrentStreams);
            api->set_grpc_resource_quota_bytes(stats.api.grpcResourceQuotaBytes);
            api->set_grpc_max_batch_items(stats.api.grpcMaxBatchItems);
            api->set_grpc_max_scan_items(stats.api.grpcMaxScanItems);
            api->set_grpc_max_history_entries(stats.api.grpcMaxHistoryEntries);
            api->set_grpc_requests_total(stats.api.grpcRequestsTotal);
            api->set_grpc_responses_total(stats.api.grpcResponsesTotal);
            api->set_grpc_active_requests(stats.api.grpcActiveRequests);
            api->set_grpc_errors_total(stats.api.grpcErrorsTotal);
            api->set_grpc_batch_put_items_total(stats.api.grpcBatchPutItemsTotal);
            api->set_grpc_batch_get_items_total(stats.api.grpcBatchGetItemsTotal);

            auto* memtable = out->mutable_memtable();
            memtable->set_shard_count(stats.memtable.shardCount);
            memtable->set_threshold_bytes_per_shard(stats.memtable.thresholdBytesPerShard);
            memtable->set_approx_bytes(stats.memtable.approxBytes);
            memtable->set_puts_applied(stats.memtable.putsApplied);
            memtable->set_removes_applied(stats.memtable.removesApplied);
            memtable->set_flushes_completed(stats.memtable.flushesCompleted);
            memtable->set_bytes_flushed(stats.memtable.bytesFlushed);

            auto* wal = out->mutable_wal();
            wal->set_enabled(stats.wal.enabled);
            wal->set_shard_count(stats.wal.shardCount);
            wal->set_entries_written(stats.wal.entriesWritten);
            wal->set_bytes_written(stats.wal.bytesWritten);
            wal->set_batches_flushed(stats.wal.batchesFlushed);
            wal->set_syncs_executed(stats.wal.syncsExecuted);
            wal->set_segment_rotations(stats.wal.segmentRotations);

            auto* blob = out->mutable_blob();
            blob->set_enabled(stats.blob.enabled);
            blob->set_threshold_bytes(stats.blob.thresholdBytes);
            blob->set_blobs_written(stats.blob.blobsWritten);
            blob->set_bytes_uncompressed(stats.blob.bytesUncompressed);
            blob->set_bytes_on_disk(stats.blob.bytesOnDisk);
            blob->set_blobs_deleted(stats.blob.blobsDeleted);
            blob->set_gc_cycles(stats.blob.gcCycles);

            auto* sst = out->mutable_sst();
            sst->set_enabled(stats.sst.enabled);
            sst->set_file_count(static_cast<uint64_t>(stats.sst.fileCount));
            sst->set_bytes(stats.sst.bytes);
            sst->set_l0_file_count(static_cast<uint64_t>(stats.sst.l0FileCount));
            sst->set_compaction_pending(stats.sst.compactionPending);
            sst->set_compactions_completed(stats.sst.compactionsCompleted);
            sst->set_files_compacted(stats.sst.filesCompacted);
            sst->set_bytes_compacted_in(stats.sst.bytesCompactedIn);
            sst->set_bytes_compacted_out(stats.sst.bytesCompactedOut);
            sst->set_l0_stalls(stats.sst.l0Stalls);
            for (const auto& level : stats.sst.levels) {
                auto* item = sst->add_levels();
                item->set_level(level.level);
                item->set_file_count(static_cast<uint64_t>(level.fileCount));
                item->set_bytes(level.bytes);
                item->set_budget_bytes(level.budgetBytes);
            }

            auto* vlog = out->mutable_vlog();
            vlog->set_enabled(stats.vlog.enabled);
            vlog->set_sync_mode(stats.vlog.syncMode);
            vlog->set_group_n(stats.vlog.groupN);
            vlog->set_group_micros(stats.vlog.groupMicros);
            vlog->set_group_bytes(stats.vlog.groupBytes);
            vlog->set_async_max_pending_bytes(stats.vlog.asyncMaxPendingBytes);
            vlog->set_indexed_keys(stats.vlog.indexedKeys);
            vlog->set_indexed_entries(stats.vlog.indexedEntries);
            vlog->set_rollback_entries(stats.vlog.rollbackEntries);
            vlog->set_pending_writes(stats.vlog.pendingWrites);
            vlog->set_pending_bytes(stats.vlog.pendingBytes);
            vlog->set_durable_bytes(stats.vlog.durableBytes);
            vlog->set_segment_count(stats.vlog.segmentCount);
            vlog->set_active_segment_bytes(stats.vlog.activeSegmentBytes);
            vlog->set_recovery_duration_micros(stats.vlog.recoveryDurationMicros);
            vlog->set_recovered_segment_count(stats.vlog.recoveredSegmentCount);
            vlog->set_recovered_entry_count(stats.vlog.recoveredEntryCount);
            vlog->set_sidecar_fallback_count(stats.vlog.sidecarFallbackCount);
            vlog->set_sidecar_rebuild_failures(stats.vlog.sidecarRebuildFailures);
            vlog->set_retention_pruned_segments(stats.vlog.retentionPrunedSegments);
            vlog->set_retention_base_entries_written(stats.vlog.retentionBaseEntriesWritten);
            vlog->set_parallel_queue_rejects(stats.vlog.parallelQueueRejects);
            vlog->set_parallel_lane_count(stats.vlog.parallelLaneCount);
            vlog->set_parallel_pending_writes(stats.vlog.parallelPendingWrites);
            vlog->set_parallel_pending_bytes(stats.vlog.parallelPendingBytes);
            vlog->set_flush_thread_running(stats.vlog.flushThreadRunning);
        }

        [[nodiscard]] std::shared_ptr<::grpc::ServerCredentials> makeCredentials(const AkkaraGRPCServerOptions& options) {
            const bool hasCert = !options.certPath.empty() || !options.keyPath.empty();
            if (!hasCert) { return ::grpc::InsecureServerCredentials(); }
            if (options.certPath.empty() || options.keyPath.empty()) {
                throw std::runtime_error("AkkaraGRPCServer: both certPath and keyPath are required for TLS");
            }

            ::grpc::SslServerCredentialsOptions sslOptions;
            sslOptions.pem_key_cert_pairs.push_back(
                ::grpc::SslServerCredentialsOptions::PemKeyCertPair{readTextFile(options.keyPath), readTextFile(options.certPath)}
            );
            if (!options.rootCertPath.empty()) {
                sslOptions.pem_root_certs = readTextFile(options.rootCertPath);
                if (options.requireClientCert) {
                    sslOptions.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
                }
            }
            return ::grpc::SslServerCredentials(sslOptions);
        }
    }

    class AkkaraGRPCServer::Impl {
        public:
            Impl(engine::AkkEngine& engine, AkkaraGRPCServerOptions options)
                : engine_{engine}, options_{std::move(options)}, service_{*this} {}

            void start() {
                if (running_.load(std::memory_order_acquire)) { return; }

                ::grpc::ServerBuilder builder;
                builder.SetMaxReceiveMessageSize(options_.maxReceiveMessageBytes);
                builder.SetMaxSendMessageSize(options_.maxSendMessageBytes);
                applyServerTuning(builder);
                builder.RegisterService(&service_);

                const std::string address = options_.bindHost + ":" + std::to_string(options_.port);
                int selectedPort = 0;
                builder.AddListeningPort(address, makeCredentials(options_), &selectedPort);

                auto server = builder.BuildAndStart();
                if (!server || selectedPort == 0) { throw std::runtime_error("AkkaraGRPCServer: failed to start on " + address); }
                server_ = std::move(server);
                running_.store(true, std::memory_order_release);
            }

            void close() {
                if (!running_.exchange(false, std::memory_order_acq_rel)) { return; }
                if (server_) {
                    server_->Shutdown();
                    server_->Wait();
                    server_.reset();
                }
            }

            [[nodiscard]] AkkaraGRPCServerStats stats() const noexcept {
                AkkaraGRPCServerStats out;
                out.running = running_.load(std::memory_order_acquire);
                out.tlsEnabled = !options_.certPath.empty();
                out.port = options_.port;
                out.requestsTotal = requestsTotal_.load(std::memory_order_relaxed);
                out.responsesTotal = responsesTotal_.load(std::memory_order_relaxed);
                out.activeRequests = activeRequests_.load(std::memory_order_relaxed);
                out.errorsTotal = errorsTotal_.load(std::memory_order_relaxed);
                out.batchPutItemsTotal = batchPutItemsTotal_.load(std::memory_order_relaxed);
                out.batchGetItemsTotal = batchGetItemsTotal_.load(std::memory_order_relaxed);
                out.workerThreads = options_.workerThreads;
                out.completionQueues = options_.completionQueues;
                out.minPollers = options_.minPollers;
                out.maxPollers = options_.maxPollers;
                out.maxConcurrentStreams = options_.maxConcurrentStreams;
                out.resourceQuotaBytes = options_.resourceQuotaBytes;
                out.maxBatchItems = maxBatchItems();
                out.maxScanItems = maxScanItems();
                out.maxHistoryEntries = maxHistoryEntries();
                return out;
            }

        private:
            class RequestScope {
                public:
                    explicit RequestScope(Impl& owner) noexcept
                        : owner_{owner} {
                        owner_.requestsTotal_.fetch_add(1, std::memory_order_relaxed);
                        owner_.activeRequests_.fetch_add(1, std::memory_order_relaxed);
                    }

                    ~RequestScope() {
                        owner_.responsesTotal_.fetch_add(1, std::memory_order_relaxed);
                        owner_.activeRequests_.fetch_sub(1, std::memory_order_relaxed);
                    }

                    RequestScope(const RequestScope&) = delete;
                    RequestScope& operator=(const RequestScope&) = delete;

                private:
                    Impl& owner_;
            };

            class Service final : public wire::AkkaraDB::Service {
                public:
                    explicit Service(Impl& owner) : owner_{owner} {}

                    ::grpc::Status Ping(::grpc::ServerContext*, const wire::PingRequest*, wire::PingResponse* response) override {
                        RequestScope requestScope{owner_};
                        response->set_message("pong");
                        return ::grpc::Status::OK;
                    }

                    ::grpc::Status Put(::grpc::ServerContext*, const wire::PutRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.put(bytes(request->key()), bytes(request->value()));
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("put failed"); }
                    }

                    ::grpc::Status PutHinted(::grpc::ServerContext*, const wire::PutHintedRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.putHinted(bytes(request->key()), bytes(request->value()), request->fp64(), request->mini_key());
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("put hinted failed"); }
                    }

                    ::grpc::Status Get(::grpc::ServerContext*, const wire::GetRequest* request, wire::GetResponse* response) override {
                        RequestScope requestScope{owner_};
                        try {
                            std::vector<uint8_t> value;
                            if (!owner_.engine_.getInto(bytes(request->key()), value)) {
                                owner_.recordError();
                                return {::grpc::StatusCode::NOT_FOUND, "key not found"};
                            }
                            setBytes(response->mutable_value(), std::span<const uint8_t>{value.data(), value.size()});
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("get failed"); }
                    }

                    ::grpc::Status Remove(::grpc::ServerContext*, const wire::RemoveRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.remove(bytes(request->key()));
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("remove failed"); }
                    }

                    ::grpc::Status RemoveHinted(::grpc::ServerContext*, const wire::RemoveHintedRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.removeHinted(bytes(request->key()), request->fp64(), request->mini_key());
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("remove hinted failed"); }
                    }

                    ::grpc::Status Exists(
                        ::grpc::ServerContext*,
                        const wire::ExistsRequest* request,
                        wire::ExistsResponse* response
                    ) override {
                        RequestScope requestScope{owner_};
                        try {
                            response->set_found(owner_.engine_.exists(bytes(request->key())));
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("exists failed"); }
                    }

                    ::grpc::Status Count(
                        ::grpc::ServerContext*,
                        const wire::CountRequest* request,
                        wire::CountResponse* response
                    ) override {
                        RequestScope requestScope{owner_};
                        try {
                            response->set_count(
                                static_cast<uint64_t>(owner_.engine_.count(bytes(request->start_key()), bytes(request->end_key())))
                            );
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("count failed"); }
                    }

                    ::grpc::Status Scan(::grpc::ServerContext*, const wire::ScanRequest* request, wire::ScanResponse* response) override {
                        RequestScope requestScope{owner_};
                        try {
                            core::BufferArena arena;
                            uint32_t emitted = 0;
                            const uint32_t limit = owner_.boundedLimit(request->limit(), owner_.maxScanItems());
                            for (const auto& record : owner_.engine_.scan(arena, bytes(request->start_key()), bytes(request->end_key()))) {
                                if (limit != 0 && emitted >= limit) {
                                    response->set_truncated(true);
                                    break;
                                }
                                auto* item = response->add_items();
                                setBytes(item->mutable_key(), record.key);
                                setBytes(item->mutable_value(), record.value);
                                ++emitted;
                            }
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("scan failed"); }
                    }

                    ::grpc::Status ScanStream(
                        ::grpc::ServerContext*,
                        const wire::ScanRequest* request,
                        ::grpc::ServerWriter<wire::ScanStreamFrame>* writer
                    ) override {
                        RequestScope requestScope{owner_};
                        try {
                            core::BufferArena arena;
                            uint32_t emitted = 0;
                            bool truncated = false;
                            const uint32_t limit = owner_.boundedLimit(request->limit(), owner_.maxScanItems());
                            for (const auto& record : owner_.engine_.scan(arena, bytes(request->start_key()), bytes(request->end_key()))) {
                                if (limit != 0 && emitted >= limit) {
                                    truncated = true;
                                    break;
                                }
                                wire::ScanStreamFrame frame;
                                auto* item = frame.mutable_item();
                                setBytes(item->mutable_key(), record.key);
                                setBytes(item->mutable_value(), record.value);
                                if (!writer->Write(frame)) {
                                    owner_.recordError();
                                    return {::grpc::StatusCode::CANCELLED, "scan stream cancelled"};
                                }
                                ++emitted;
                            }

                            wire::ScanStreamFrame endFrame;
                            auto* end = endFrame.mutable_end();
                            end->set_emitted_count(emitted);
                            end->set_truncated(truncated);
                            if (!writer->Write(endFrame)) {
                                owner_.recordError();
                                return {::grpc::StatusCode::CANCELLED, "scan stream cancelled"};
                            }
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("scan stream failed"); }
                    }

                    ::grpc::Status GetAt(::grpc::ServerContext*, const wire::GetAtRequest* request, wire::GetResponse* response) override {
                        RequestScope requestScope{owner_};
                        try {
                            auto value = owner_.engine_.getAt(bytes(request->key()), request->seq());
                            if (!value) {
                                owner_.recordError();
                                return {::grpc::StatusCode::NOT_FOUND, "key not found at seq"};
                            }
                            setBytes(response->mutable_value(), std::span<const uint8_t>{value->data(), value->size()});
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("getAt failed"); }
                    }

                    ::grpc::Status History(
                        ::grpc::ServerContext*,
                        const wire::HistoryRequest* request,
                        wire::HistoryResponse* response
                    ) override {
                        RequestScope requestScope{owner_};
                        try {
                            uint32_t emitted = 0;
                            const uint32_t limit = owner_.maxHistoryEntries();
                            for (const auto& entry : owner_.engine_.history(bytes(request->key()))) {
                                if (limit != 0 && emitted >= limit) {
                                    response->set_truncated(true);
                                    break;
                                }
                                auto* item = response->add_entries();
                                item->set_seq(entry.seq);
                                item->set_source_node_id(entry.sourceNodeId);
                                item->set_timestamp_ns(entry.timestampNs);
                                item->set_flags(entry.flags);
                                setBytes(item->mutable_value(), std::span<const uint8_t>{entry.value.data(), entry.value.size()});
                                ++emitted;
                            }
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("history failed"); }
                    }

                    ::grpc::Status HistoryStream(
                        ::grpc::ServerContext*,
                        const wire::HistoryRequest* request,
                        ::grpc::ServerWriter<wire::HistoryStreamFrame>* writer
                    ) override {
                        RequestScope requestScope{owner_};
                        try {
                            uint32_t emitted = 0;
                            bool truncated = false;
                            const uint32_t limit = owner_.maxHistoryEntries();
                            for (const auto& entry : owner_.engine_.history(bytes(request->key()))) {
                                if (limit != 0 && emitted >= limit) {
                                    truncated = true;
                                    break;
                                }
                                wire::HistoryStreamFrame frame;
                                auto* item = frame.mutable_item();
                                item->set_seq(entry.seq);
                                item->set_source_node_id(entry.sourceNodeId);
                                item->set_timestamp_ns(entry.timestampNs);
                                item->set_flags(entry.flags);
                                setBytes(item->mutable_value(), std::span<const uint8_t>{entry.value.data(), entry.value.size()});
                                if (!writer->Write(frame)) {
                                    owner_.recordError();
                                    return {::grpc::StatusCode::CANCELLED, "history stream cancelled"};
                                }
                                ++emitted;
                            }

                            wire::HistoryStreamFrame endFrame;
                            auto* end = endFrame.mutable_end();
                            end->set_emitted_count(emitted);
                            end->set_truncated(truncated);
                            if (!writer->Write(endFrame)) {
                                owner_.recordError();
                                return {::grpc::StatusCode::CANCELLED, "history stream cancelled"};
                            }
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("history stream failed"); }
                    }

                    ::grpc::Status RollbackTo(::grpc::ServerContext*, const wire::RollbackToRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.rollbackTo(request->target_seq());
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("rollbackTo failed"); }
                    }

                    ::grpc::Status RollbackKey(::grpc::ServerContext*, const wire::RollbackKeyRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.rollbackKey(bytes(request->key()), request->target_seq());
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("rollbackKey failed"); }
                    }

                    ::grpc::Status BatchPut(::grpc::ServerContext*, const wire::BatchPutRequest* request, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            if (static_cast<uint32_t>(request->items_size()) > owner_.maxBatchItems()) {
                                return owner_.invalidArgument("too many batch put items");
                            }
                            std::vector<engine::AkkEngine::BatchPutEntry> entries;
                            entries.reserve(static_cast<size_t>(request->items_size()));
                            for (const auto& item : request->items()) { entries.push_back({bytes(item.key()), bytes(item.value())}); }
                            owner_.engine_.putBatch(std::span<const engine::AkkEngine::BatchPutEntry>{entries.data(), entries.size()});
                            owner_.batchPutItemsTotal_.fetch_add(entries.size(), std::memory_order_relaxed);
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("batch put failed"); }
                    }

                    ::grpc::Status BatchGet(
                        ::grpc::ServerContext*,
                        const wire::BatchGetRequest* request,
                        wire::BatchGetResponse* response
                    ) override {
                        RequestScope requestScope{owner_};
                        try {
                            if (static_cast<uint32_t>(request->keys_size()) > owner_.maxBatchItems()) {
                                return owner_.invalidArgument("too many batch get items");
                            }
                            std::vector<std::span<const uint8_t>> keys;
                            keys.reserve(static_cast<size_t>(request->keys_size()));
                            for (const auto& key : request->keys()) { keys.push_back(bytes(key)); }

                            const auto values = owner_.engine_.getBatch(
                                std::span<const std::span<const uint8_t>>{keys.data(), keys.size()}
                            );
                            for (const auto& value : values) {
                                auto* item = response->add_items();
                                item->set_status(value.found ? wire::ITEM_STATUS_OK : wire::ITEM_STATUS_NOT_FOUND);
                                if (value.found) {
                                    setBytes(item->mutable_value(), std::span<const uint8_t>{value.value.data(), value.value.size()});
                                }
                            }
                            owner_.batchGetItemsTotal_.fetch_add(keys.size(), std::memory_order_relaxed);
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("batch get failed"); }
                    }

                    ::grpc::Status ForceSync(::grpc::ServerContext*, const wire::Empty*, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.forceSync();
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("force sync failed"); }
                    }

                    ::grpc::Status ForceFlush(::grpc::ServerContext*, const wire::Empty*, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.forceFlush();
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("force flush failed"); }
                    }

                    ::grpc::Status RunBlobGc(::grpc::ServerContext*, const wire::Empty*, wire::Empty*) override {
                        RequestScope requestScope{owner_};
                        try {
                            owner_.engine_.runBlobGc();
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("run blob gc failed"); }
                    }

                    ::grpc::Status Stats(::grpc::ServerContext*, const wire::StatsRequest*, wire::StatsResponse* response) override {
                        RequestScope requestScope{owner_};
                        try {
                            copyStats(owner_.engine_.stats(), response->mutable_engine());
                            return ::grpc::Status::OK;
                        }
                        catch (const std::exception& e) { return owner_.errorStatus(e.what()); }
                        catch (...) { return owner_.errorStatus("stats failed"); }
                    }

                private:
                    Impl& owner_;
            };

            void recordError() noexcept { errorsTotal_.fetch_add(1, std::memory_order_relaxed); }

            [[nodiscard]] uint32_t maxBatchItems() const noexcept { return options_.maxBatchItems == 0 ? 4096u : options_.maxBatchItems; }

            [[nodiscard]] uint32_t maxScanItems() const noexcept { return options_.maxScanItems == 0 ? 4096u : options_.maxScanItems; }

            [[nodiscard]] uint32_t maxHistoryEntries() const noexcept {
                return options_.maxHistoryEntries == 0 ? 4096u : options_.maxHistoryEntries;
            }

            [[nodiscard]] uint32_t boundedLimit(uint32_t requested, uint32_t configuredMax) const noexcept {
                if (configuredMax == 0) { return requested; }
                if (requested == 0) { return configuredMax; }
                return std::min(requested, configuredMax);
            }

            void applyServerTuning(::grpc::ServerBuilder& builder) const {
                if (options_.completionQueues != 0) {
                    builder.SetSyncServerOption(
                        ::grpc::ServerBuilder::SyncServerOption::NUM_CQS,
                        static_cast<int>(std::min<uint32_t>(
                            options_.completionQueues,
                            static_cast<uint32_t>(std::numeric_limits<int>::max())
                        ))
                    );
                }
                if (options_.minPollers != 0) {
                    builder.SetSyncServerOption(
                        ::grpc::ServerBuilder::SyncServerOption::MIN_POLLERS,
                        static_cast<int>(std::min<uint32_t>(options_.minPollers, static_cast<uint32_t>(std::numeric_limits<int>::max())))
                    );
                }
                if (options_.maxPollers != 0) {
                    builder.SetSyncServerOption(
                        ::grpc::ServerBuilder::SyncServerOption::MAX_POLLERS,
                        static_cast<int>(std::min<uint32_t>(options_.maxPollers, static_cast<uint32_t>(std::numeric_limits<int>::max())))
                    );
                }
                if (options_.maxConcurrentStreams != 0) {
                    builder.AddChannelArgument(
                        GRPC_ARG_MAX_CONCURRENT_STREAMS,
                        static_cast<int>(std::min<uint32_t>(
                            options_.maxConcurrentStreams,
                            static_cast<uint32_t>(std::numeric_limits<int>::max())
                        ))
                    );
                }
                if (options_.workerThreads != 0 || options_.resourceQuotaBytes != 0) {
                    ::grpc::ResourceQuota quota{"akkaradb-grpc"};
                    if (options_.workerThreads != 0) {
                        quota.SetMaxThreads(
                            static_cast<int>(std::min<uint32_t>(
                                options_.workerThreads,
                                static_cast<uint32_t>(std::numeric_limits<int>::max())
                            ))
                        );
                    }
                    if (options_.resourceQuotaBytes != 0) {
                        quota.Resize(
                            static_cast<size_t>(std::min<uint64_t>(
                                options_.resourceQuotaBytes,
                                static_cast<uint64_t>(std::numeric_limits<size_t>::max())
                            ))
                        );
                    }
                    builder.SetResourceQuota(quota);
                }
            }

            [[nodiscard]] ::grpc::Status errorStatus(std::string message) {
                recordError();
                return {::grpc::StatusCode::INTERNAL, std::move(message)};
            }

            [[nodiscard]] ::grpc::Status invalidArgument(std::string message) {
                recordError();
                return {::grpc::StatusCode::INVALID_ARGUMENT, std::move(message)};
            }

            engine::AkkEngine& engine_;
            AkkaraGRPCServerOptions options_;
            Service service_;
            std::unique_ptr<::grpc::Server> server_;
            std::atomic<bool> running_{false};
            std::atomic<uint64_t> requestsTotal_{0};
            std::atomic<uint64_t> responsesTotal_{0};
            std::atomic<uint64_t> activeRequests_{0};
            std::atomic<uint64_t> errorsTotal_{0};
            std::atomic<uint64_t> batchPutItemsTotal_{0};
            std::atomic<uint64_t> batchGetItemsTotal_{0};
    };

    std::unique_ptr<AkkaraGRPCServer> AkkaraGRPCServer::create(engine::AkkEngine& engine, AkkaraGRPCServerOptions options) {
        return std::unique_ptr<AkkaraGRPCServer>{new AkkaraGRPCServer{std::make_unique<Impl>(engine, std::move(options))}};
    }

    AkkaraGRPCServer::AkkaraGRPCServer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    AkkaraGRPCServer::~AkkaraGRPCServer() { close(); }

    void AkkaraGRPCServer::start() { impl_->start(); }

    void AkkaraGRPCServer::close() { if (impl_) { impl_->close(); } }

    AkkaraGRPCServerStats AkkaraGRPCServer::stats() const noexcept { return impl_ ? impl_->stats() : AkkaraGRPCServerStats{}; }
}

namespace {
    [[nodiscard]] akkaradb::grpcapi::AkkaraGRPCServerOptions makeGRPCOptions(
        const akkaradb::engine::AkkEngineOptions::ApiOptions& options
    ) {
        akkaradb::grpcapi::AkkaraGRPCServerOptions grpcOptions;
        grpcOptions.bindHost = options.bindHost;
        grpcOptions.port = options.grpcPort;
        grpcOptions.maxReceiveMessageBytes = static_cast<int>(options.tcpMaxPendingResponseBytes == 0
                                                                  ? 64ULL * 1024ULL * 1024ULL
                                                                  : std::min<uint64_t>(
                                                                      options.tcpMaxPendingResponseBytes,
                                                                      static_cast<uint64_t>(std::numeric_limits<int>::max())
                                                                  ));
        grpcOptions.maxSendMessageBytes = grpcOptions.maxReceiveMessageBytes;
        grpcOptions.workerThreads = options.grpcWorkerThreads;
        grpcOptions.completionQueues = options.grpcCompletionQueues;
        grpcOptions.minPollers = options.grpcMinPollers;
        grpcOptions.maxPollers = options.grpcMaxPollers;
        grpcOptions.maxConcurrentStreams = options.grpcMaxConcurrentStreams;
        grpcOptions.resourceQuotaBytes = options.grpcResourceQuotaBytes;
        grpcOptions.maxBatchItems = options.grpcMaxBatchItems;
        grpcOptions.maxScanItems = options.grpcMaxScanItems;
        grpcOptions.maxHistoryEntries = options.grpcMaxHistoryEntries;
        if (options.transportMode == akkaradb::engine::AkkEngineOptions::ApiTransportMode::TLS) {
            grpcOptions.certPath = options.tls.certPath;
            grpcOptions.keyPath = options.tls.keyPath;
            grpcOptions.rootCertPath = options.tls.caPath;
            grpcOptions.requireClientCert = options.tls.verifyPeer && !options.tls.caPath.empty();
        }
        return grpcOptions;
    }

    class GRPCApiTransport final : public akkaradb::engine::server::IAkkApiTransport {
        public:
            GRPCApiTransport(akkaradb::engine::AkkEngine& engine, const akkaradb::engine::AkkEngineOptions::ApiOptions& options)
                : server_{akkaradb::grpcapi::AkkaraGRPCServer::create(engine, makeGRPCOptions(options))} {}

            void start() override { server_->start(); }
            void close() override { if (server_) { server_->close(); } }

            [[nodiscard]] akkaradb::engine::EngineStats::ApiStats stats() const noexcept override {
                akkaradb::engine::EngineStats::ApiStats out;
                if (!server_) { return out; }
                const auto grpcStats = server_->stats();
                out.enabled = grpcStats.running;
                out.grpcEnabled = true;
                out.grpcTlsEnabled = grpcStats.tlsEnabled;
                out.grpcPort = grpcStats.port;
                out.grpcWorkerThreads = grpcStats.workerThreads;
                out.grpcCompletionQueues = grpcStats.completionQueues;
                out.grpcMinPollers = grpcStats.minPollers;
                out.grpcMaxPollers = grpcStats.maxPollers;
                out.grpcMaxConcurrentStreams = grpcStats.maxConcurrentStreams;
                out.grpcResourceQuotaBytes = grpcStats.resourceQuotaBytes;
                out.grpcMaxBatchItems = grpcStats.maxBatchItems;
                out.grpcMaxScanItems = grpcStats.maxScanItems;
                out.grpcMaxHistoryEntries = grpcStats.maxHistoryEntries;
                out.grpcRequestsTotal = grpcStats.requestsTotal;
                out.grpcResponsesTotal = grpcStats.responsesTotal;
                out.grpcActiveRequests = grpcStats.activeRequests;
                out.grpcErrorsTotal = grpcStats.errorsTotal;
                out.grpcBatchPutItemsTotal = grpcStats.batchPutItemsTotal;
                out.grpcBatchGetItemsTotal = grpcStats.batchGetItemsTotal;
                return out;
            }

        private:
            std::unique_ptr<akkaradb::grpcapi::AkkaraGRPCServer> server_;
    };
}

extern "C" AKKARADB_GRPC_API bool akkaradb_api_grpc_register() noexcept {
    return akkaradb::engine::server::registerAkkApiTransportFactory(
        akkaradb::engine::AkkEngineOptions::ApiBackend::GRPC,
        [](
        akkaradb::engine::AkkEngine& engine,
        const akkaradb::engine::AkkEngineOptions::ApiOptions& options
    ) -> std::unique_ptr<akkaradb::engine::server::IAkkApiTransport> {
            return std::make_unique<GRPCApiTransport>(engine, options);
        }
    );
}
