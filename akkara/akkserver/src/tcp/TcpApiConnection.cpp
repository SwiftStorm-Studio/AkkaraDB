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

#include "akk/engine/server/tcp/detail/TcpApiConnection.hpp"

#include "akk/engine/server/tcp/detail/TcpApiFrameReader.hpp"
#include "akk/engine/server/tcp/detail/TcpApiPayloadCodec.hpp"
#include "akk/engine/server/ApiFraming.hpp"

#include <algorithm>
#include <cstring>
#include <span>
#include <vector>

namespace akkaradb::engine::server::tcp {
    namespace {
        constexpr uint32_t kDefaultMaxBatchItems = 4096u;
    }

    ConnectionHandler::ConnectionHandler(AkkEngine& engine, const AkkEngineOptions::ApiOptions& options, ConnectionCounters counters)
        : engine_{engine}, options_{options}, counters_{counters} {}

    uint32_t ConnectionHandler::maxBatchItems() const noexcept {
        return options_.tcpMaxBatchItems == 0 ? kDefaultMaxBatchItems : options_.tcpMaxBatchItems;
    }

    void ConnectionHandler::handle(detail::Connection& connection, const std::atomic<bool>& running) {
        BufferedInput reader;
        std::vector<uint8_t> outputBuffer;
        std::vector<uint8_t> responseBuffer;
        std::vector<uint8_t> writeBuffer;
        std::vector<ApiBatchPutItem> batchPutItems;
        std::vector<AkkEngine::BatchPutEntry> enginePutItems;
        std::vector<std::span<const uint8_t>> batchGetKeys;
        std::vector<AkkEngine::BatchGetResult> engineGetResults;
        std::vector<ApiBatchGetResult> wireGetResults;
        std::vector<AkkEngine::ScanRecordView> scanRecords;

        const auto flushPending = [&]() -> bool {
            if (writeBuffer.empty()) { return true; }
            const size_t bytes = writeBuffer.size();
            if (!connection.sendAll(writeBuffer.data(), writeBuffer.size())) {
                counters_.backpressureDisconnectsTotal.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            counters_.bytesSentTotal.fetch_add(bytes, std::memory_order_relaxed);
            writeBuffer.clear();
            return true;
        };

        const auto appendResponse = [&](const std::vector<uint8_t>& response) -> bool {
            if (response.empty()) { return true; }

            const uint64_t maxPending = options_.tcpMaxPendingResponseBytes;
            if (maxPending > 0 && !writeBuffer.empty() && writeBuffer.size() + response.size() > maxPending) {
                counters_.backpressureFlushesTotal.fetch_add(1, std::memory_order_relaxed);
                if (!flushPending()) { return false; }
            }

            counters_.responsesTotal.fetch_add(1, std::memory_order_relaxed);
            if (maxPending > 0 && response.size() > maxPending) {
                if (!connection.sendAll(response.data(), response.size())) {
                    counters_.backpressureDisconnectsTotal.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                counters_.bytesSentTotal.fetch_add(response.size(), std::memory_order_relaxed);
                return true;
            }

            writeBuffer.insert(writeBuffer.end(), response.begin(), response.end());
            if (maxPending > 0 && writeBuffer.size() >= maxPending) {
                counters_.backpressureFlushesTotal.fetch_add(1, std::memory_order_relaxed);
                return flushPending();
            }
            return true;
        };

        const auto sendResponsePayload = [&](ApiStatus status, uint32_t requestId, std::span<const uint8_t> payload) -> bool {
            encodeResponse(status, requestId, payload, responseBuffer);
            return appendResponse(responseBuffer);
        };

        const auto processFrame = [&](const RequestFrame& frame) -> bool {
            counters_.requestsTotal.fetch_add(1, std::memory_order_relaxed);
            counters_.bytesReceivedTotal.fetch_add(frame.wireSize, std::memory_order_relaxed);
            responseBuffer.clear();

            if (frame.receivedCrc != crc32c(frame.key, frame.value)) {
                counters_.crcErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                encodeError(frame.header.requestId, responseBuffer);
                return false;
            }

            try {
                switch (frame.header.opcode) {
                    case ApiOp::PUT: engine_.put(frame.key, frame.value);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::GET: outputBuffer.clear();
                        if (engine_.getInto(frame.key, outputBuffer)) {
                            encodeResponse(
                                ApiStatus::OK,
                                frame.header.requestId,
                                std::span<const uint8_t>{outputBuffer.data(), outputBuffer.size()},
                                responseBuffer
                            );
                        }
                        else { encodeResponse(ApiStatus::NOT_FOUND, frame.header.requestId, {}, responseBuffer); }
                        break;
                    case ApiOp::REMOVE: engine_.remove(frame.key);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::GET_AT: {
                        uint64_t seq = 0;
                        if (!readU64(frame.value, seq)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        auto historicalValue = engine_.getAt(frame.key, seq);
                        if (historicalValue) {
                            encodeResponse(
                                ApiStatus::OK,
                                frame.header.requestId,
                                std::span<const uint8_t>{historicalValue->data(), historicalValue->size()},
                                responseBuffer
                            );
                        }
                        else { encodeResponse(ApiStatus::NOT_FOUND, frame.header.requestId, {}, responseBuffer); }
                        break;
                    }
                    case ApiOp::BATCH_PUT: {
                        uint32_t count = 0;
                        const uint32_t maxBatch = maxBatchItems();
                        if (frame.header.keyLen != 0 || !readBatchCount(frame.value, count) || count > maxBatch ||
                            !decodeBatchPut(frame.value, maxBatch, batchPutItems)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        enginePutItems.clear();
                        enginePutItems.reserve(batchPutItems.size());
                        for (const ApiBatchPutItem& item : batchPutItems) { enginePutItems.push_back({item.key, item.value}); }
                        engine_.putBatch(std::span<const AkkEngine::BatchPutEntry>{enginePutItems.data(), enginePutItems.size()});
                        counters_.batchPutItemsTotal.fetch_add(batchPutItems.size(), std::memory_order_relaxed);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    }
                    case ApiOp::BATCH_GET: {
                        uint32_t count = 0;
                        const uint32_t maxBatch = maxBatchItems();
                        if (frame.header.keyLen != 0 || !readBatchCount(frame.value, count) || count > maxBatch ||
                            !decodeBatchGet(frame.value, maxBatch, batchGetKeys)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        engineGetResults = engine_.getBatch(std::span<const std::span<const uint8_t>>{batchGetKeys.data(), batchGetKeys.size()});
                        wireGetResults.clear();
                        wireGetResults.reserve(engineGetResults.size());
                        for (const auto& result : engineGetResults) {
                            wireGetResults.push_back(
                                ApiBatchGetResult{result.found ? ApiStatus::OK : ApiStatus::NOT_FOUND, {result.value.data(), result.value.size()}}
                            );
                        }
                        counters_.batchGetItemsTotal.fetch_add(batchGetKeys.size(), std::memory_order_relaxed);
                        encodeBatchGetResponse(frame.header.requestId, {wireGetResults.data(), wireGetResults.size()}, responseBuffer);
                        break;
                    }
                    case ApiOp::PING: {
                        static constexpr std::string_view pong = "pong";
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {reinterpret_cast<const uint8_t*>(pong.data()), pong.size()}, responseBuffer);
                        break;
                    }
                    case ApiOp::EXISTS:
                        encodeBoolPayload(engine_.exists(frame.key), outputBuffer);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()}, responseBuffer);
                        break;
                    case ApiOp::COUNT:
                        encodeU64Payload(static_cast<uint64_t>(engine_.count(frame.key, frame.value)), outputBuffer);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()}, responseBuffer);
                        break;
                    case ApiOp::SCAN: {
                        uint32_t limit = 0;
                        std::span<const uint8_t> endKey;
                        if (!readScanPayload(frame.value, limit, endKey)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        core::BufferArena arena;
                        scanRecords.clear();
                        bool truncated = false;
                        uint32_t emitted = 0;
                        for (const auto& record : engine_.scan(arena, frame.key, endKey)) {
                            if (limit != 0 && emitted >= limit) {
                                truncated = true;
                                break;
                            }
                            scanRecords.push_back(record);
                            ++emitted;
                        }
                        encodeScanPayload({scanRecords.data(), scanRecords.size()}, truncated, outputBuffer);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()}, responseBuffer);
                        break;
                    }
                    case ApiOp::HISTORY: {
                        const auto entries = engine_.history(frame.key);
                        encodeHistoryPayload({entries.data(), entries.size()}, outputBuffer);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()}, responseBuffer);
                        break;
                    }
                    case ApiOp::SCAN_STREAM: {
                        uint32_t limit = 0;
                        std::span<const uint8_t> endKey;
                        if (!readScanPayload(frame.value, limit, endKey)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        core::BufferArena arena;
                        uint32_t emitted = 0;
                        bool truncated = false;
                        for (const auto& record : engine_.scan(arena, frame.key, endKey)) {
                            if (limit != 0 && emitted >= limit) {
                                truncated = true;
                                break;
                            }
                            encodeScanStreamPayload(record, outputBuffer);
                            if (!sendResponsePayload(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()})) {
                                responseBuffer.clear();
                                return false;
                            }
                            ++emitted;
                        }

                        encodeStreamEndPayload(emitted, truncated, outputBuffer);
                        if (!sendResponsePayload(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()})) {
                            responseBuffer.clear();
                            return false;
                        }
                        responseBuffer.clear();
                        break;
                    }
                    case ApiOp::HISTORY_STREAM: {
                        uint32_t limit = 0;
                        if (!readOptionalLimit(frame.value, limit)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }

                        uint32_t emitted = 0;
                        bool truncated = false;
                        for (const auto& entry : engine_.history(frame.key)) {
                            if (limit != 0 && emitted >= limit) {
                                truncated = true;
                                break;
                            }
                            encodeHistoryStreamPayload(entry, outputBuffer);
                            if (!sendResponsePayload(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()})) {
                                responseBuffer.clear();
                                return false;
                            }
                            ++emitted;
                        }

                        encodeStreamEndPayload(emitted, truncated, outputBuffer);
                        if (!sendResponsePayload(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()})) {
                            responseBuffer.clear();
                            return false;
                        }
                        responseBuffer.clear();
                        break;
                    }
                    case ApiOp::ROLLBACK_TO: {
                        uint64_t targetSeq = 0;
                        if (!readU64(frame.value, targetSeq)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        engine_.rollbackTo(targetSeq);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    }
                    case ApiOp::ROLLBACK_KEY: {
                        uint64_t targetSeq = 0;
                        if (!readU64(frame.value, targetSeq)) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        engine_.rollbackKey(frame.key, targetSeq);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    }
                    case ApiOp::FORCE_SYNC: engine_.forceSync();
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::FORCE_FLUSH: engine_.forceFlush();
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::RUN_BLOB_GC:
                        if (frame.header.keyLen != 0 || frame.header.valLen != 0) {
                            counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                            encodeError(frame.header.requestId, responseBuffer);
                            return false;
                        }
                        engine_.runBlobGc();
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {}, responseBuffer);
                        break;
                    case ApiOp::STATS:
                        encodeStatsPayload(engine_.stats(), outputBuffer);
                        encodeResponse(ApiStatus::OK, frame.header.requestId, {outputBuffer.data(), outputBuffer.size()}, responseBuffer);
                        break;
                    default:
                        counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                        encodeError(frame.header.requestId, responseBuffer);
                        return false;
                }
            }
            catch (...) {
                counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                encodeError(frame.header.requestId, responseBuffer);
                return false;
            }

            return true;
        };

        const uint32_t pipelineLimit = std::max<uint32_t>(1, options_.tcpPipelineBatchLimit);
        while (running.load(std::memory_order_relaxed)) {
            RequestFrame frame;
            FrameReadStatus status = reader.readFrame(connection, frame, true);
            if (status == FrameReadStatus::CLOSED || status == FrameReadStatus::NEED_MORE) { break; }
            if (status == FrameReadStatus::INVALID) {
                counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                counters_.bytesReceivedTotal.fetch_add(frame.wireSize, std::memory_order_relaxed);
                if (frame.requestIdUsable) {
                    encodeError(frame.header.requestId, responseBuffer);
                    (void)appendResponse(responseBuffer);
                    (void)flushPending();
                }
                break;
            }

            bool keepConnection = processFrame(frame);
            reader.consume(frame.wireSize);
            if (!appendResponse(responseBuffer)) { break; }

            uint32_t framesInBatch = 1;
            while (keepConnection && framesInBatch < pipelineLimit) {
                RequestFrame pipelined;
                status = reader.readFrame(connection, pipelined, false);
                if (status == FrameReadStatus::NEED_MORE || status == FrameReadStatus::CLOSED) { break; }
                if (status == FrameReadStatus::INVALID) {
                    counters_.protocolErrorsTotal.fetch_add(1, std::memory_order_relaxed);
                    counters_.bytesReceivedTotal.fetch_add(pipelined.wireSize, std::memory_order_relaxed);
                    keepConnection = false;
                    if (pipelined.requestIdUsable) {
                        encodeError(pipelined.header.requestId, responseBuffer);
                        (void)appendResponse(responseBuffer);
                    }
                    break;
                }

                keepConnection = processFrame(pipelined);
                reader.consume(pipelined.wireSize);
                if (!appendResponse(responseBuffer)) { return; }
                ++framesInBatch;
            }

            if (framesInBatch > 1) { counters_.pipelineBatchesTotal.fetch_add(1, std::memory_order_relaxed); }
            if (!flushPending() || !keepConnection) { break; }
        }
    }
}
