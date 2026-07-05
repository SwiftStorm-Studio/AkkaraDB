    [[nodiscard]] AkkaraDB::Options readOptions(JNIEnv* env, jobject optionsBuffer) {
        BytesReader in(readDirectBuffer(env, optionsBuffer));
        const auto version = in.u8();
        if (version != 1 && version != 2 && version != 3 && version != 4) { throw std::runtime_error("Unsupported AkkaraDB options payload version"); }

        AkkaraDB::Options options;
        options.mode = startupModeFromOrdinal(static_cast<jint>(in.i32()));
        options.dataDir = readUtf8String(in);

        const auto memtableThresholdPerShard = static_cast<jlong>(in.i64());
        if (memtableThresholdPerShard >= 0) {
            options.overrides.memtableThresholdPerShard = checkedCast<size_t>(memtableThresholdPerShard, "memtableThresholdPerShard");
        }

        assignOptionalBool(options.overrides.versionLogEnabled, static_cast<jint>(in.i32()), "versionLogEnabled");

        const auto sstCodec = static_cast<jint>(in.i32());
        if (sstCodec >= 0) { options.overrides.sstCodec = codecFromOrdinal(sstCodec); }

        const auto blobCodec = static_cast<jint>(in.i32());
        if (blobCodec >= 0) { options.overrides.blobCodec = codecFromOrdinal(blobCodec); }

        const auto blobThresholdBytes = static_cast<jlong>(in.i64());
        if (blobThresholdBytes >= 0) {
            options.overrides.blobThresholdBytes = checkedCast<uint64_t>(blobThresholdBytes, "blobThresholdBytes");
        }

        assignOptionalBool(options.overrides.sstPromoteReads, static_cast<jint>(in.i32()), "sstPromoteReads");

        const auto sstBloomBitsPerKey = static_cast<jlong>(in.i64());
        if (sstBloomBitsPerKey >= 0) {
            options.overrides.sstBloomBitsPerKey = checkedCast<size_t>(sstBloomBitsPerKey, "sstBloomBitsPerKey");
        }

        const auto maxL0SstFiles = static_cast<jlong>(in.i64());
        if (maxL0SstFiles >= 0) {
            options.overrides.maxL0SstFiles = checkedCast<size_t>(maxL0SstFiles, "maxL0SstFiles");
        }

        if (version >= 2) {
            const auto hasApi = static_cast<jint>(in.i32());
            if (hasApi != 0) {
                AkkaraDB::Options::ApiOptions api;
                const auto backendCount = static_cast<jint>(in.i32());
                if (backendCount < 0) { throw std::runtime_error("api.backends count must be >= 0"); }
                api.backends.reserve(static_cast<size_t>(backendCount));
                for (jint i = 0; i < backendCount; ++i) {
                    api.backends.push_back(apiBackendFromOrdinal(static_cast<jint>(in.i32())));
                }
                api.bindHost = readUtf8String(in);
                api.httpPort = checkedCast<uint16_t>(static_cast<jlong>(in.i32()), "api.httpPort");
                api.tcpPort = checkedCast<uint16_t>(static_cast<jlong>(in.i32()), "api.tcpPort");
                api.grpcPort = checkedCast<uint16_t>(static_cast<jlong>(in.i32()), "api.grpcPort");
                api.transportMode = apiTransportModeFromOrdinal(static_cast<jint>(in.i32()));
                if (const auto value = readOptionalUtf8String(in)) { api.serverBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.transportBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.httpBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.tcpBackendPath = *value; }
                if (const auto value = readOptionalUtf8String(in)) { api.grpcBackendPath = *value; }
                if (version >= 3) {
                    api.httpMaxBatchItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.httpMaxBatchItems");
                    api.httpMaxScanItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.httpMaxScanItems");
                    api.httpMaxHistoryEntries = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.httpMaxHistoryEntries");
                    api.httpMaxContentLength = checkedCast<uint64_t>(static_cast<jlong>(in.i64()), "api.httpMaxContentLength");
                    api.grpcWorkerThreads = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcWorkerThreads");
                    api.grpcCompletionQueues = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcCompletionQueues");
                    api.grpcMinPollers = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMinPollers");
                    api.grpcMaxPollers = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxPollers");
                    api.grpcMaxConcurrentStreams = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxConcurrentStreams");
                    api.grpcResourceQuotaBytes = checkedCast<uint64_t>(static_cast<jlong>(in.i64()), "api.grpcResourceQuotaBytes");
                    api.grpcMaxBatchItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxBatchItems");
                    api.grpcMaxScanItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxScanItems");
                    api.grpcMaxHistoryEntries = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.grpcMaxHistoryEntries");
                    api.tcpIoBackend = apiIoBackendFromOrdinal(static_cast<jint>(in.i32()));
                    api.tcpWorkerThreads = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpWorkerThreads");
                    api.tcpAcceptQueueLimit = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpAcceptQueueLimit");
                    api.tcpAcceptQueueTimeoutMs = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpAcceptQueueTimeoutMs");
                    api.tcpListenBacklog = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpListenBacklog");
                    api.tcpRecvBufferBytes = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpRecvBufferBytes");
                    api.tcpSendBufferBytes = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpSendBufferBytes");
                    api.tcpPipelineBatchLimit = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpPipelineBatchLimit");
                    api.tcpMaxBatchItems = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpMaxBatchItems");
                    api.tcpMaxPendingResponseBytes = checkedCast<uint64_t>(static_cast<jlong>(in.i64()), "api.tcpMaxPendingResponseBytes");
                    api.tcpReadTimeoutMs = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpReadTimeoutMs");
                    api.tcpWriteTimeoutMs = checkedCast<uint32_t>(static_cast<jlong>(in.i32()), "api.tcpWriteTimeoutMs");
                    api.tcpNoDelay = boolFromInt(static_cast<jint>(in.i32()), "api.tcpNoDelay");
                    api.tcpKeepAlive = boolFromInt(static_cast<jint>(in.i32()), "api.tcpKeepAlive");
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.certPath = *value; }
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.keyPath = *value; }
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.caPath = *value; }
                    if (version >= 4) {
                        if (const auto value = readOptionalBytes(in)) { api.tls.psk = std::move(*value); }
                    }
                    if (const auto value = readOptionalUtf8String(in)) { api.tls.pskIdentity = *value; }
                    api.tls.verifyPeer = boolFromInt(static_cast<jint>(in.i32()), "api.tls.verifyPeer");
                }
                options.api = std::move(api);
            }
        }

        if (!in.eof()) { throw std::runtime_error("Trailing bytes in AkkaraDB options payload"); }
        if (options.dataDir.empty() && options.mode != StartupMode::ULTRA_FAST) {
            throw std::runtime_error("dataDir must not be blank unless mode is ULTRA_FAST");
        }
        return options;
    }

    template <typename F>
    auto guard(JNIEnv* env, F&& f) -> decltype(f()) {
        try { return f(); }
        catch (const std::exception& ex) {
            throwRuntime(env, ex);
            using R = decltype(f());
            if constexpr (std::is_pointer_v<R>) { return nullptr; }
            else if constexpr (std::is_same_v<R, jboolean>) { return JNI_FALSE; }
            else if constexpr (std::is_integral_v<R>) { return 0; }
            else { return R{}; }
        }
    }
