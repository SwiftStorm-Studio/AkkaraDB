    [[nodiscard]] jobject makeRow(JNIEnv* env, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        const auto& ids = bindings(env);
        jobject keyBuffer = makeDirectView(env, key);
        if (keyBuffer == nullptr || env->ExceptionCheck()) { return nullptr; }
        jobject valueBuffer = makeDirectView(env, value);
        if (valueBuffer == nullptr || env->ExceptionCheck()) { return nullptr; }
        return env->NewObject(ids.rowViewClass, ids.rowViewCtor, keyBuffer, valueBuffer, nullptr);
    }

    [[nodiscard]] jbyteArray makeByteArray(JNIEnv* env, std::span<const uint8_t> bytes) {
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
            throw std::runtime_error("AkkaraDB JNI byte array is too large");
        }
        jbyteArray out = env->NewByteArray(static_cast<jsize>(bytes.size()));
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        if (!bytes.empty()) {
            env->SetByteArrayRegion(out, 0, static_cast<jsize>(bytes.size()), reinterpret_cast<const jbyte*>(bytes.data()));
            if (env->ExceptionCheck()) { return nullptr; }
        }
        return out;
    }

    [[nodiscard]] jobject makeVersionEntry(JNIEnv* env, const akkaradb::engine::VersionEntry& entry) {
        const auto& ids = bindings(env);
        jbyteArray rawValue = makeByteArray(env, entry.value);
        if (rawValue == nullptr && env->ExceptionCheck()) { return nullptr; }
        return env->NewObject(
            ids.versionEntryClass,
            ids.versionEntryCtor,
            static_cast<jlong>(entry.seq),
            static_cast<jlong>(entry.sourceNodeId),
            static_cast<jlong>(entry.timestampNs),
            static_cast<jbyte>(entry.flags),
            rawValue
        );
    }

    jfieldID requireField(JNIEnv* env, jclass cls, const char* name, const char* sig) {
        jfieldID field = env->GetFieldID(cls, name, sig);
        if (field == nullptr || env->ExceptionCheck()) { throw std::runtime_error(std::string("Failed to resolve field: ") + name); }
        return field;
    }

    void setBooleanField(JNIEnv* env, jobject target, jclass cls, const char* name, bool value) {
        env->SetBooleanField(target, requireField(env, cls, name, "Z"), value ? JNI_TRUE : JNI_FALSE);
    }

    void setIntField(JNIEnv* env, jobject target, jclass cls, const char* name, jint value) {
        env->SetIntField(target, requireField(env, cls, name, "I"), value);
    }

    void setLongField(JNIEnv* env, jobject target, jclass cls, const char* name, jlong value) {
        env->SetLongField(target, requireField(env, cls, name, "J"), value);
    }

    void setObjectField(JNIEnv* env, jobject target, jclass cls, const char* name, const char* sig, jobject value) {
        env->SetObjectField(target, requireField(env, cls, name, sig), value);
    }

    void readDirectBufferArray(JNIEnv* env, jobjectArray array, std::vector<std::span<const uint8_t>>& out) {
        if (array == nullptr) { throw std::runtime_error("JNI buffer array must not be null"); }
        const jsize size = env->GetArrayLength(array);
        out.clear();
        out.reserve(static_cast<size_t>(size));
        for (jsize i = 0; i < size; ++i) {
            jobject buffer = env->GetObjectArrayElement(array, i);
            if (buffer == nullptr) { throw std::runtime_error("JNI buffer array contains null entry"); }
            out.push_back(readDirectBuffer(env, buffer));
            env->DeleteLocalRef(buffer);
        }
    }

    [[nodiscard]] jobject makeBatchGetResult(JNIEnv* env, const AkkEngine::BatchGetResult& result) {
        const auto& ids = bindings(env);
        jobject out = env->NewObject(ids.batchGetResultClass, ids.batchGetResultCtor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        jbyteArray value = makeByteArray(env, result.value);
        if (value == nullptr && env->ExceptionCheck()) { return nullptr; }
        setBooleanField(env, out, ids.batchGetResultClass, "found", result.found);
        setObjectField(env, out, ids.batchGetResultClass, "valueBytes", "[B", value);
        return out;
    }

    [[nodiscard]] jobject makeLevelStats(JNIEnv* env, const akkaradb::engine::LevelStats& stats) {
        jclass cls = env->FindClass("dev/swiftstorm/akkaradb/engine/LevelStats");
        if (cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
        if (ctor == nullptr) { return nullptr; }
        jobject out = env->NewObject(cls, ctor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        setIntField(env, out, cls, "level", static_cast<jint>(stats.level));
        setLongField(env, out, cls, "fileCount", static_cast<jlong>(stats.fileCount));
        setLongField(env, out, cls, "bytes", static_cast<jlong>(stats.bytes));
        setLongField(env, out, cls, "budgetBytes", static_cast<jlong>(stats.budgetBytes));
        return out;
    }

    [[nodiscard]] jobject makeApiStats(JNIEnv* env, const akkaradb::engine::EngineStats::ApiStats& stats) {
        jclass cls = env->FindClass("dev/swiftstorm/akkaradb/engine/ApiStats");
        if (cls == nullptr) { return nullptr; }
        jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
        if (ctor == nullptr) { return nullptr; }
        jobject out = env->NewObject(cls, ctor);
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
        setBooleanField(env, out, cls, "enabled", stats.enabled);
        setBooleanField(env, out, cls, "httpEnabled", stats.httpEnabled);
        setBooleanField(env, out, cls, "httpTlsEnabled", stats.httpTlsEnabled);
        setIntField(env, out, cls, "httpPort", static_cast<jint>(stats.httpPort));
        setLongField(env, out, cls, "httpMaxBatchItems", static_cast<jlong>(stats.httpMaxBatchItems));
        setLongField(env, out, cls, "httpMaxScanItems", static_cast<jlong>(stats.httpMaxScanItems));
        setLongField(env, out, cls, "httpMaxHistoryEntries", static_cast<jlong>(stats.httpMaxHistoryEntries));
        setLongField(env, out, cls, "httpMaxContentLength", static_cast<jlong>(stats.httpMaxContentLength));
        setLongField(env, out, cls, "httpConnectionsAcceptedTotal", static_cast<jlong>(stats.httpConnectionsAcceptedTotal));
        setLongField(env, out, cls, "httpConnectionsClosedTotal", static_cast<jlong>(stats.httpConnectionsClosedTotal));
        setLongField(env, out, cls, "httpConnectionsActive", static_cast<jlong>(stats.httpConnectionsActive));
        setLongField(env, out, cls, "httpRequestsTotal", static_cast<jlong>(stats.httpRequestsTotal));
        setLongField(env, out, cls, "httpResponsesTotal", static_cast<jlong>(stats.httpResponsesTotal));
        setLongField(env, out, cls, "httpBytesReceivedTotal", static_cast<jlong>(stats.httpBytesReceivedTotal));
        setLongField(env, out, cls, "httpBytesSentTotal", static_cast<jlong>(stats.httpBytesSentTotal));
        setLongField(env, out, cls, "httpProtocolErrorsTotal", static_cast<jlong>(stats.httpProtocolErrorsTotal));
        setLongField(env, out, cls, "httpErrorsTotal", static_cast<jlong>(stats.httpErrorsTotal));
        setLongField(env, out, cls, "httpBatchPutItemsTotal", static_cast<jlong>(stats.httpBatchPutItemsTotal));
        setLongField(env, out, cls, "httpBatchGetItemsTotal", static_cast<jlong>(stats.httpBatchGetItemsTotal));
        setBooleanField(env, out, cls, "tcpEnabled", stats.tcpEnabled);
        setBooleanField(env, out, cls, "tcpTlsEnabled", stats.tcpTlsEnabled);
        setIntField(env, out, cls, "tcpIoBackend", static_cast<jint>(stats.tcpIoBackend));
        setLongField(env, out, cls, "tcpWorkerThreads", static_cast<jlong>(stats.tcpWorkerThreads));
        setLongField(env, out, cls, "tcpAcceptQueueLimit", static_cast<jlong>(stats.tcpAcceptQueueLimit));
        setLongField(env, out, cls, "tcpAcceptQueueTimeoutMs", static_cast<jlong>(stats.tcpAcceptQueueTimeoutMs));
        setLongField(env, out, cls, "tcpListenBacklog", static_cast<jlong>(stats.tcpListenBacklog));
        setLongField(env, out, cls, "tcpReadTimeoutMs", static_cast<jlong>(stats.tcpReadTimeoutMs));
        setLongField(env, out, cls, "tcpWriteTimeoutMs", static_cast<jlong>(stats.tcpWriteTimeoutMs));
        setLongField(env, out, cls, "tcpConnectionsAcceptedTotal", static_cast<jlong>(stats.tcpConnectionsAcceptedTotal));
        setLongField(env, out, cls, "tcpConnectionsClosedTotal", static_cast<jlong>(stats.tcpConnectionsClosedTotal));
        setLongField(env, out, cls, "tcpConnectionsActive", static_cast<jlong>(stats.tcpConnectionsActive));
        setLongField(env, out, cls, "tcpAcceptQueueDepth", static_cast<jlong>(stats.tcpAcceptQueueDepth));
        setLongField(env, out, cls, "tcpAcceptQueuePeakDepth", static_cast<jlong>(stats.tcpAcceptQueuePeakDepth));
        setLongField(env, out, cls, "tcpAcceptQueueRejectedTotal", static_cast<jlong>(stats.tcpAcceptQueueRejectedTotal));
        setLongField(env, out, cls, "tcpAcceptQueueExpiredTotal", static_cast<jlong>(stats.tcpAcceptQueueExpiredTotal));
        setLongField(env, out, cls, "tcpRequestsTotal", static_cast<jlong>(stats.tcpRequestsTotal));
        setLongField(env, out, cls, "tcpResponsesTotal", static_cast<jlong>(stats.tcpResponsesTotal));
        setLongField(env, out, cls, "tcpBytesReceivedTotal", static_cast<jlong>(stats.tcpBytesReceivedTotal));
        setLongField(env, out, cls, "tcpBytesSentTotal", static_cast<jlong>(stats.tcpBytesSentTotal));
        setLongField(env, out, cls, "tcpProtocolErrorsTotal", static_cast<jlong>(stats.tcpProtocolErrorsTotal));
        setLongField(env, out, cls, "tcpCrcErrorsTotal", static_cast<jlong>(stats.tcpCrcErrorsTotal));
        setLongField(env, out, cls, "tcpPipelineBatchesTotal", static_cast<jlong>(stats.tcpPipelineBatchesTotal));
        setLongField(env, out, cls, "tcpBackpressureFlushesTotal", static_cast<jlong>(stats.tcpBackpressureFlushesTotal));
        setLongField(env, out, cls, "tcpBackpressureDisconnectsTotal", static_cast<jlong>(stats.tcpBackpressureDisconnectsTotal));
        setLongField(env, out, cls, "tcpBatchPutItemsTotal", static_cast<jlong>(stats.tcpBatchPutItemsTotal));
        setLongField(env, out, cls, "tcpBatchGetItemsTotal", static_cast<jlong>(stats.tcpBatchGetItemsTotal));
        setBooleanField(env, out, cls, "grpcEnabled", stats.grpcEnabled);
        setBooleanField(env, out, cls, "grpcTlsEnabled", stats.grpcTlsEnabled);
        setIntField(env, out, cls, "grpcPort", static_cast<jint>(stats.grpcPort));
        setLongField(env, out, cls, "grpcWorkerThreads", static_cast<jlong>(stats.grpcWorkerThreads));
        setLongField(env, out, cls, "grpcCompletionQueues", static_cast<jlong>(stats.grpcCompletionQueues));
        setLongField(env, out, cls, "grpcMinPollers", static_cast<jlong>(stats.grpcMinPollers));
        setLongField(env, out, cls, "grpcMaxPollers", static_cast<jlong>(stats.grpcMaxPollers));
        setLongField(env, out, cls, "grpcMaxConcurrentStreams", static_cast<jlong>(stats.grpcMaxConcurrentStreams));
        setLongField(env, out, cls, "grpcResourceQuotaBytes", static_cast<jlong>(stats.grpcResourceQuotaBytes));
        setLongField(env, out, cls, "grpcMaxBatchItems", static_cast<jlong>(stats.grpcMaxBatchItems));
        setLongField(env, out, cls, "grpcMaxScanItems", static_cast<jlong>(stats.grpcMaxScanItems));
        setLongField(env, out, cls, "grpcMaxHistoryEntries", static_cast<jlong>(stats.grpcMaxHistoryEntries));
        setLongField(env, out, cls, "grpcRequestsTotal", static_cast<jlong>(stats.grpcRequestsTotal));
        setLongField(env, out, cls, "grpcResponsesTotal", static_cast<jlong>(stats.grpcResponsesTotal));
        setLongField(env, out, cls, "grpcActiveRequests", static_cast<jlong>(stats.grpcActiveRequests));
        setLongField(env, out, cls, "grpcErrorsTotal", static_cast<jlong>(stats.grpcErrorsTotal));
        setLongField(env, out, cls, "grpcBatchPutItemsTotal", static_cast<jlong>(stats.grpcBatchPutItemsTotal));
        setLongField(env, out, cls, "grpcBatchGetItemsTotal", static_cast<jlong>(stats.grpcBatchGetItemsTotal));
        return out;
    }

    template <typename TStats>
