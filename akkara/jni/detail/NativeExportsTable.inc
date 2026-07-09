    JNIEXPORT jstring JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeLibrary_nativeJniCompatLine(JNIEnv* env, jclass) {
    return env->NewStringUTF(AKKARADB_JNI_COMPAT_LINE);
}

    JNIEXPORT jstring JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeLibrary_nativeJniAbi(JNIEnv* env, jclass) {
    return env->NewStringUTF(AKKARADB_JNI_ABI);
}

    JNIEXPORT jstring JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeLibrary_nativeRequiredNativeGeneration(JNIEnv* env, jclass) {
    return env->NewStringUTF(AKKARADB_REQUIRED_NATIVE_GENERATION);
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpen(JNIEnv* env, jclass, jobject optionsBuffer) {
    return guard(
        env,
        [&]() -> jlong {
            auto options = readOptions(env, optionsBuffer);
            auto db = AkkaraDB::open(std::move(options));
            return toHandle(db.release());
        }
    );
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeOpenTable(
    JNIEnv* env,
    jobject,
    jlong engineHandle,
    jobject tablePrefix,
    jobject schemaBytes
) {
    return guard(
        env,
        [&]() -> jlong {
            auto* db = dbFrom(engineHandle);
            if (db == nullptr) { throw std::runtime_error("JNIPackedTable engine handle is null"); }
            auto table = std::make_unique<NativeTable>();
            table->db = db;
            const auto prefix = readDirectBuffer(env, tablePrefix);
            table->tablePrefix.assign(prefix.begin(), prefix.end());
            table->schema = parseRootSchema(readDirectBuffer(env, schemaBytes));
            return toHandle(table.release());
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeRegisterIndex(
    JNIEnv* env,
    jobject,
    jlong tableHandle,
    jstring fieldName,
    jobject indexPrefix
) {
    guard(
        env,
        [&]() {
            auto* table = tableFrom(tableHandle);
            if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
            const std::string name = readJavaString(env, fieldName);
            for (auto& index : table->indexes) { if (index.fieldName == name) { return 0; } }
            NativeIndexDef index;
            index.fieldName = name;
            const auto prefix = readDirectBuffer(env, indexPrefix);
            index.prefix.assign(prefix.begin(), prefix.end());
            index.column = resolveColumn(table->schema, name);
            table->indexes.push_back(std::move(index));
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativePutIndexed(
    JNIEnv* env,
    jobject,
    jlong tableHandle,
    jobject pkBytes,
    jobject value
) {
    guard(
        env,
        [&]() {
            auto* table = tableFrom(tableHandle);
            if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
            const auto pk = readDirectBuffer(env, pkBytes);
            const auto rowValue = readDirectBuffer(env, value);
            const auto pkKey = concatBytes(table->tablePrefix, pk);
            auto oldValue = table->db->engine().get(pkKey);
            if (oldValue) { updateIndexEntries(*table, pk, *oldValue, true); }
            table->db->engine().put(pkKey, rowValue);
            updateIndexEntries(*table, pk, rowValue, false);
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeRemoveIndexed(
    JNIEnv* env,
    jobject,
    jlong tableHandle,
    jobject pkBytes
) {
    guard(
        env,
        [&]() {
            auto* table = tableFrom(tableHandle);
            if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
            const auto pk = readDirectBuffer(env, pkBytes);
            const auto pkKey = concatBytes(table->tablePrefix, pk);
            auto oldValue = table->db->engine().get(pkKey);
            if (oldValue) { updateIndexEntries(*table, pk, *oldValue, true); }
            table->db->engine().remove(pkKey);
            return 0;
        }
    );
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeOpenIndexScan(
    JNIEnv* env,
    jobject,
    jlong tableHandle,
    jstring fieldName,
    jobject fieldBytes
) {
    return guard(
        env,
        [&]() -> jlong {
            auto* table = tableFrom(tableHandle);
            if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
            const auto& index = requireIndex(*table, readJavaString(env, fieldName));
            const auto field = readDirectBuffer(env, fieldBytes);
            std::vector<uint8_t> startKey;
            buildIndexSearchPrefix(index.prefix, field, startKey);
            std::vector<uint8_t> endKey = startKey;
            if (!incrementLexicographic(endKey)) { endKey.clear(); }

            auto cursor = std::make_unique<ScanCursor>();
            cursor->indexLookup = std::make_unique<ScanCursor::IndexLookup>();
            cursor->indexLookup->db = table->db;
            cursor->indexLookup->tablePrefix = table->tablePrefix;
            cursor->indexLookup->searchPrefixSize = startKey.size();
            cursor->rows = table->db->engine().scan(cursor->arena, startKey, endKey);
            cursor->it = cursor->rows.begin();
            return toHandle(cursor.release());
        }
    );
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeOpenQueryIndexScan(
    JNIEnv* env,
    jobject,
    jlong tableHandle,
    jstring fieldName,
    jobject fieldBytes,
    jobject queryBytes,
    jobject schemaBytes
) {
    return guard(
        env,
        [&]() -> jlong {
            auto* table = tableFrom(tableHandle);
            if (table == nullptr) { throw std::runtime_error("JNIPackedTable handle is null"); }
            const auto& index = requireIndex(*table, readJavaString(env, fieldName));
            const auto field = readDirectBuffer(env, fieldBytes);
            std::vector<uint8_t> startKey;
            buildIndexSearchPrefix(index.prefix, field, startKey);
            std::vector<uint8_t> endKey = startKey;
            if (!incrementLexicographic(endKey)) { endKey.clear(); }

            auto cursor = std::make_unique<ScanCursor>();
            cursor->indexLookup = std::make_unique<ScanCursor::IndexLookup>();
            cursor->indexLookup->db = table->db;
            cursor->indexLookup->tablePrefix = table->tablePrefix;
            cursor->indexLookup->searchPrefixSize = startKey.size();
            cursor->query = std::make_unique<QueryProgram>(
                parseQuery(readDirectBuffer(env, queryBytes), readDirectBuffer(env, schemaBytes))
            );
            cursor->rows = table->db->engine().scan(cursor->arena, startKey, endKey);
            cursor->it = cursor->rows.begin();
            return toHandle(cursor.release());
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_JNIPackedTable_nativeCloseTable(JNIEnv* env, jobject, jlong tableHandle) {
    guard(
        env,
        [&]() {
            std::unique_ptr<NativeTable> table{tableFrom(tableHandle)};
            return 0;
        }
    );
}
