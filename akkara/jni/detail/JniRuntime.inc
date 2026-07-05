    struct JniBindings {
        jclass runtimeExceptionClass = nullptr;
        jclass byteBufferClass = nullptr;
        jmethodID byteBufferAllocateDirect = nullptr;
        jmethodID byteBufferAsReadOnly = nullptr;
        jclass rowViewClass = nullptr;
        jmethodID rowViewCtor = nullptr;
        jclass versionEntryClass = nullptr;
        jmethodID versionEntryCtor = nullptr;
        jclass batchGetResultClass = nullptr;
        jmethodID batchGetResultCtor = nullptr;
    };

    [[nodiscard]] const JniBindings& bindings(JNIEnv* env) {
        static JniBindings cached;
        static std::once_flag once;
        static std::exception_ptr initError;

        std::call_once(
            once,
            [&]() {
                try {
                    jclass runtimeExceptionLocal = env->FindClass("java/lang/RuntimeException");
                    if (runtimeExceptionLocal == nullptr) { throw std::runtime_error("Failed to resolve RuntimeException class"); }
                    cached.runtimeExceptionClass = static_cast<jclass>(env->NewGlobalRef(runtimeExceptionLocal));
                    env->DeleteLocalRef(runtimeExceptionLocal);
                    if (cached.runtimeExceptionClass == nullptr) { throw std::runtime_error("Failed to cache RuntimeException class"); }

                    jclass byteBufferLocal = env->FindClass("java/nio/ByteBuffer");
                    if (byteBufferLocal == nullptr) { throw std::runtime_error("Failed to resolve ByteBuffer class"); }
                    cached.byteBufferClass = static_cast<jclass>(env->NewGlobalRef(byteBufferLocal));
                    env->DeleteLocalRef(byteBufferLocal);
                    if (cached.byteBufferClass == nullptr) { throw std::runtime_error("Failed to cache ByteBuffer class"); }
                    cached.byteBufferAllocateDirect = env->GetStaticMethodID(
                        cached.byteBufferClass,
                        "allocateDirect",
                        "(I)Ljava/nio/ByteBuffer;"
                    );
                    cached.byteBufferAsReadOnly = env->GetMethodID(
                        cached.byteBufferClass,
                        "asReadOnlyBuffer",
                        "()Ljava/nio/ByteBuffer;"
                    );
                    if (cached.byteBufferAllocateDirect == nullptr || cached.byteBufferAsReadOnly == nullptr) {
                        throw std::runtime_error("Failed to resolve ByteBuffer methods");
                    }

                    jclass rowViewLocal = env->FindClass("dev/swiftstorm/akkaradb/engine/RowView");
                    if (rowViewLocal == nullptr) { throw std::runtime_error("Failed to resolve RowView class"); }
                    cached.rowViewClass = static_cast<jclass>(env->NewGlobalRef(rowViewLocal));
                    env->DeleteLocalRef(rowViewLocal);
                    if (cached.rowViewClass == nullptr) { throw std::runtime_error("Failed to cache RowView class"); }
                    cached.rowViewCtor = env->GetMethodID(
                        cached.rowViewClass,
                        "<init>",
                        "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Lkotlin/jvm/internal/DefaultConstructorMarker;)V"
                    );
                    if (cached.rowViewCtor == nullptr) { throw std::runtime_error("Failed to resolve RowView constructor"); }

                    jclass versionEntryLocal = env->FindClass("dev/swiftstorm/akkaradb/engine/VersionEntry");
                    if (versionEntryLocal == nullptr) { throw std::runtime_error("Failed to resolve VersionEntry class"); }
                    cached.versionEntryClass = static_cast<jclass>(env->NewGlobalRef(versionEntryLocal));
                    env->DeleteLocalRef(versionEntryLocal);
                    if (cached.versionEntryClass == nullptr) { throw std::runtime_error("Failed to cache VersionEntry class"); }
                    cached.versionEntryCtor = env->GetMethodID(
                        cached.versionEntryClass,
                        "<init>",
                        "(JJJB[B)V"
                    );
                    if (cached.versionEntryCtor == nullptr) { throw std::runtime_error("Failed to resolve VersionEntry constructor"); }

                    jclass batchGetResultLocal = env->FindClass("dev/swiftstorm/akkaradb/engine/BatchGetResult");
                    if (batchGetResultLocal == nullptr) { throw std::runtime_error("Failed to resolve BatchGetResult class"); }
                    cached.batchGetResultClass = static_cast<jclass>(env->NewGlobalRef(batchGetResultLocal));
                    env->DeleteLocalRef(batchGetResultLocal);
                    if (cached.batchGetResultClass == nullptr) { throw std::runtime_error("Failed to cache BatchGetResult class"); }
                    cached.batchGetResultCtor = env->GetMethodID(
                        cached.batchGetResultClass,
                        "<init>",
                        "()V"
                    );
                    if (cached.batchGetResultCtor == nullptr) { throw std::runtime_error("Failed to resolve BatchGetResult constructor"); }
                }
                catch (...) { initError = std::current_exception(); }
            }
        );

        if (initError != nullptr) { std::rethrow_exception(initError); }
        return cached;
    }

    [[nodiscard]] AkkaraDB* dbFrom(jlong handle) noexcept { return reinterpret_cast<AkkaraDB*>(static_cast<std::uintptr_t>(handle)); }

    struct NativeValueView {
        BufferArena arena;
        std::span<const uint8_t> value;
    };

    [[nodiscard]] jlong toHandle(void* ptr) noexcept { return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(ptr)); }

    void throwRuntime(JNIEnv* env, const std::exception& ex) {
        env->ThrowNew(bindings(env).runtimeExceptionClass, ex.what());
    }

    [[nodiscard]] std::span<const uint8_t> readDirectBuffer(JNIEnv* env, jobject buffer) {
        if (buffer == nullptr) { return {}; }
        void* raw = env->GetDirectBufferAddress(buffer);
        const jlong capacity = env->GetDirectBufferCapacity(buffer);
        if (raw == nullptr || capacity < 0) { throw std::runtime_error("AkkaraDB JNI requires a direct ByteBuffer"); }
        return {reinterpret_cast<const uint8_t*>(raw), static_cast<size_t>(capacity)};
    }

    [[nodiscard]] jobject makeDirectBuffer(JNIEnv* env, std::span<const uint8_t> bytes) {
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<jint>::max())) {
            throw std::runtime_error("AkkaraDB JNI buffer is too large for a JVM ByteBuffer");
        }
        const auto& ids = bindings(env);
        jobject out = env->CallStaticObjectMethod(ids.byteBufferClass, ids.byteBufferAllocateDirect, static_cast<jint>(bytes.size()));
        if (out == nullptr || env->ExceptionCheck()) { return nullptr; }

        if (!bytes.empty()) {
            void* raw = env->GetDirectBufferAddress(out);
            if (raw == nullptr) { throw std::runtime_error("AkkaraDB JNI failed to allocate a direct ByteBuffer"); }
            std::memcpy(raw, bytes.data(), bytes.size());
        }
        return out;
    }

    [[nodiscard]] jobject makeDirectView(JNIEnv* env, std::span<const uint8_t> bytes) {
        if (bytes.empty()) { return makeDirectBuffer(env, bytes); }
        if (bytes.size() > static_cast<size_t>(std::numeric_limits<jint>::max())) {
            throw std::runtime_error("AkkaraDB JNI buffer is too large for a JVM ByteBuffer");
        }
        const auto& ids = bindings(env);
        jobject direct = env->NewDirectByteBuffer(const_cast<uint8_t*>(bytes.data()), static_cast<jlong>(bytes.size()));
        if (direct == nullptr) { return nullptr; }
        return env->CallObjectMethod(direct, ids.byteBufferAsReadOnly);
    }

    [[nodiscard]] StartupMode startupModeFromOrdinal(jint value) {
        switch (value) {
            case 0: return StartupMode::ULTRA_FAST;
            case 1: return StartupMode::FAST;
            case 2: return StartupMode::NORMAL;
            case 3: return StartupMode::DURABLE;
            default: throw std::runtime_error("Invalid AkkaraDB StartupMode ordinal");
        }
    }

    [[nodiscard]] Codec codecFromOrdinal(jint value) {
        switch (value) {
            case 0: return Codec::NONE;
            case 1: return Codec::ZSTD;
            default: throw std::runtime_error("Invalid AkkaraDB Codec ordinal");
        }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions::ApiBackend apiBackendFromOrdinal(jint value) {
        switch (value) {
            case 0: return akkaradb::engine::AkkEngineOptions::ApiBackend::HTTP;
            case 1: return akkaradb::engine::AkkEngineOptions::ApiBackend::TCP;
            case 2: return akkaradb::engine::AkkEngineOptions::ApiBackend::GRPC;
            default: throw std::runtime_error("Invalid AkkaraDB ApiBackend ordinal");
        }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions::ApiTransportMode apiTransportModeFromOrdinal(jint value) {
        switch (value) {
            case 0: return akkaradb::engine::AkkEngineOptions::ApiTransportMode::TLS;
            case 1: return akkaradb::engine::AkkEngineOptions::ApiTransportMode::PLAIN;
            default: throw std::runtime_error("Invalid AkkaraDB ApiTransportMode ordinal");
        }
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions::ApiIoBackend apiIoBackendFromOrdinal(jint value) {
        switch (value) {
            case 0: return akkaradb::engine::AkkEngineOptions::ApiIoBackend::AUTO;
            case 1: return akkaradb::engine::AkkEngineOptions::ApiIoBackend::THREAD_POOL;
            default: throw std::runtime_error("Invalid AkkaraDB ApiIoBackend ordinal");
        }
    }

    [[nodiscard]] bool boolFromInt(jint value, const char* name) {
        switch (value) {
            case 0: return false;
            case 1: return true;
            default: throw std::runtime_error(std::string("Invalid boolean value for ") + name);
        }
    }

    void assignOptionalBool(std::optional<bool>& target, jint value, const char* name) {
        switch (value) {
            case -1: return;
            case 0: target = false;
                return;
            case 1: target = true;
                return;
            default: throw std::runtime_error(std::string("Invalid boolean override for ") + name);
        }
    }

    template <typename T>
    [[nodiscard]] T checkedCast(jlong value, const char* name) {
        if (value < 0) { throw std::runtime_error(std::string(name) + " must be >= 0"); }
        using LimitT = std::numeric_limits<T>;
        using UnsignedWide = unsigned long long;
        const auto raw = static_cast<UnsignedWide>(value);
        if (raw > static_cast<UnsignedWide>(LimitT::max())) {
            throw std::runtime_error(std::string(name) + " is out of range");
        }
        return static_cast<T>(value);
    }
