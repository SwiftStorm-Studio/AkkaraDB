# AkkaraDB Native 繧｢繝ｼ繧ｭ繝・け繝√Ε

縺薙・譁・嶌縺ｯ縲、kkaraDB 縺ｮ native C++23 engine 縺ｮ蜈ｨ菴灘ワ繧定ｪｬ譏弱＠縺ｾ縺吶Ｗ5 technical specification 縺ｮ陬懷勧雉・侭縺ｧ縺ゅｊ縲～SPEC.md` 縺梧ｭ｣遒ｺ縺ｪ format 縺ｨ compatibility rule 繧貞ｮ夂ｾｩ縺吶ｋ source of truth 縺ｧ縺ゅｋ縺ｮ縺ｫ蟇ｾ縺励※縲√％縺薙〒縺ｯ蜷・subsystem 縺ｮ雋ｬ蜍吶→ data flow 繧定ｪｬ譏弱＠縺ｾ縺吶・
## 逶ｮ谺｡

- [讎りｦ‐(#讎りｦ・
- [繧ｹ繝医Ξ繝ｼ繧ｸ讒区・](#繧ｹ繝医Ξ繝ｼ繧ｸ讒区・)
- [荳ｻ隕√さ繝ｳ繝昴・繝阪Φ繝・(#荳ｻ隕√さ繝ｳ繝昴・繝阪Φ繝・
  - [蜈ｬ髢・API 螻､](#蜈ｬ髢・api-螻､)
  - [AkkEngine](#akkengine)
  - [繝ｬ繧ｳ繝ｼ繝峨→繧ｭ繝ｼ縺ｮ繝｢繝・Ν](#繝ｬ繧ｳ繝ｼ繝峨→繧ｭ繝ｼ縺ｮ繝｢繝・Ν)
  - [繝｡繝｢繝ｪ邂｡逅・(#繝｡繝｢繝ｪ邂｡逅・
  - [MemTable](#memtable)
  - [譖ｸ縺崎ｾｼ縺ｿ蜈郁｡後Ο繧ｰ](#譖ｸ縺崎ｾｼ縺ｿ蜈郁｡後Ο繧ｰ)
  - [Blob 繝槭ロ繝ｼ繧ｸ繝｣](#blob-繝槭ロ繝ｼ繧ｸ繝｣)
  - [SST 繝槭ロ繝ｼ繧ｸ繝｣](#sst-繝槭ロ繝ｼ繧ｸ繝｣)
  - [繝槭ル繝輔ぉ繧ｹ繝・(#繝槭ル繝輔ぉ繧ｹ繝・
  - [繝舌・繧ｸ繝ｧ繝ｳ繝ｭ繧ｰ](#繝舌・繧ｸ繝ｧ繝ｳ繝ｭ繧ｰ)
  - [API 繧ｵ繝ｼ繝舌・](#api-繧ｵ繝ｼ繝舌・)
  - [Cluster Runtime 縺ｨ TLS](#cluster-runtime-縺ｨ-tls)
- [繝・・繧ｿ繝輔Ο繝ｼ](#繝・・繧ｿ繝輔Ο繝ｼ)
- [繝ｪ繧ｫ繝舌Μ縺ｨ繧ｷ繝｣繝・ヨ繝繧ｦ繝ｳ](#繝ｪ繧ｫ繝舌Μ縺ｨ繧ｷ繝｣繝・ヨ繝繧ｦ繝ｳ)
- [荳ｦ陦梧ｧ繝｢繝・Ν](#荳ｦ陦梧ｧ繝｢繝・Ν)
- [襍ｷ蜍輔Δ繝ｼ繝云(#襍ｷ蜍輔Δ繝ｼ繝・

---

## 讎りｦ・
AkkaraDB native 縺ｯ縲、kkaraDB 縺ｫ縺翫￠繧・canonical 縺ｪ C++23 storage engine 縺ｧ縺吶Ｃinary-safe 縺ｪ key/value 繧・LSM-tree 縺ｧ菫晏ｭ倥＠縲『rite 縺ｯ縺ｾ縺・in-memory 縺ｮ MemTable 縺ｫ蜈･繧翫‥urable 讒区・縺ｧ縺ｯ WAL segment 縺ｫ霑ｽ險倥＆繧後’lush 縺ｫ繧医▲縺ｦ immutable 縺ｪ SST file 縺御ｽ懊ｉ繧後〕evel 髢薙〒 compaction 縺輔ｌ縺ｾ縺吶・
native engine 縺ｫ縺ｯ縲∵怙蟆乗ｧ区・縺ｮ LSM 螳溯｣・ｒ雜・∴繧・subsystem 繧ょ性縺ｾ繧後∪縺吶・
- Blob Manager 縺ｫ繧医ｋ large value 縺ｮ螟夜Κ蛹・- Version Log 縺ｫ繧医ｋ莉ｻ諢上・ per-key history
- BinPack serialization 縺ｨ secondary index key 繧剃ｽｿ縺・typed C++ table
- Kotlin/JVM layer 縺御ｽｿ縺・JNI entry point
- 邨・∩霎ｼ縺ｿ縺ｮ HTTP / binary TCP API server
- standalone縲［irror縲《tripe deployment 蜷代￠縺ｮ cluster / replication primitive
- mbedTLS 縺ｫ繧医ｋ TLS transport support

蜈ｨ菴灘ワ縺ｯ谺｡縺ｮ騾壹ｊ縺ｧ縺吶・
```text
+---------------------------------------------------------------+
|                         Public API                            |
|                                                               |
|   AkkaraDB      PackedTable<&T::id>      engine::AkkEngine     |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
|                           AkkEngine                           |
|                                                               |
|  +-------------------+      +------------------------------+  |
|  | MemTable          |      | WAL Writer                   |  |
|  | - N shards        |      | - sharded segments           |  |
|  | - BPTree/ART/etc. |      | - sync/async/off             |  |
|  +---------+---------+      +------------------------------+  |
|            |                                                  |
|            | flush                                            |
|            v                                                  |
|  +-------------------+      +------------------------------+  |
|  | SST Manager       |<---->| Manifest                     |  |
|  | - L0..L6 default  |      | - SST lifecycle log          |  |
|  | - bloom + index   |      | - compaction commits         |  |
|  +-------------------+      +------------------------------+  |
|                                                               |
|  +-------------------+      +------------------------------+  |
|  | Blob Manager      |      | VersionLog                   |  |
|  | - values >= limit |      | - point-in-time lookup       |  |
|  | - .blob files     |      | - rollback support           |  |
|  +-------------------+      +------------------------------+  |
|                                                               |
|  +-------------------+      +------------------------------+  |
|  | API Server        |      | Cluster Runtime              |  |
|  | - HTTP/TCP        |      | - primary/replica            |  |
|  | - AK5 protocol    |      | - mirror/stripe routing      |  |
|  +-------------------+      +------------------------------+  |
+---------------------------------------------------------------+
```

---

## 繧ｹ繝医Ξ繝ｼ繧ｸ讒区・

`paths.dataDir` 縺瑚ｨｭ螳壹＆繧後※縺・ｋ蝣ｴ蜷医∵悴險ｭ螳壹・ component path 縺ｯ縺昴％縺九ｉ閾ｪ蜍募ｰ主・縺輔ｌ縺ｾ縺吶・
```text
{dataDir}/
|-- wal/             WAL segment files
|-- sstable/         SST levels
|   |-- L0/
|   |-- L1/
|   |-- ...
|   `-- L6/
|-- blobs/           Externalized value payloads
|-- manifest.akmf    SST lifecycle and checkpoint metadata
|-- history.akvlog   Version history when enabled
|-- cluster.akcc     Cluster topology when enabled
|-- cluster.akmf     Node-local cluster runtime event log when enabled
`-- node.id          Persistent node identity
```

豁｣遒ｺ縺ｪ binary format 縺ｯ `SPEC.md` 縺ｫ縺ゅｊ縺ｾ縺吶ゅい繝ｼ繧ｭ繝・け繝√Ε荳企㍾隕√↑縺ｮ縺ｯ縲∝推 persistent subsystem 縺ｮ雋ｬ蜍吶′蛻・屬縺輔ｌ縺ｦ縺・ｋ縺薙→縺ｧ縺吶・
| Component | 蠖ｹ蜑ｲ |
|---|---|
| WAL | crash 蠕後↓ acknowledged / pending write 繧・replay 縺吶ｋ |
| SST | immutable 縺ｪ sorted key/value record 繧剃ｿ晄戟縺吶ｋ |
| Manifest | SST lifecycle縲…heckpoint縲…luster runtime event 繧定ｿｽ霍｡縺吶ｋ |
| Blob files | MemTable / WAL / SST payload 縺ｮ螟悶↓螟ｧ縺阪↑蛟､繧剃ｿ晏ｭ倥☆繧・|
| Version Log | point-in-time read 縺ｨ rollback 逕ｨ縺ｮ螻･豁ｴ繧剃ｿ晏ｭ倥☆繧・|
| Cluster config | runtime TLS path 縺ｧ縺ｯ縺ｪ縺・durable 縺ｪ cluster topology 繧剃ｿ晏ｭ倥☆繧・|

---

## 荳ｻ隕√さ繝ｳ繝昴・繝阪Φ繝・
### 蜈ｬ髢・API 螻､

native repository 縺ｫ縺ｯ 2 谿ｵ髫弱・ API 縺後≠繧翫∪縺吶・
`engine::AkkEngine` 縺ｯ byte 謖・髄縺ｮ core API 縺ｧ縺吶Ｓaw byte span 縺ｮ key/value 繧貞女縺大叙繧翫｝oint read縲『rite縲〉emove縲《can縲”istory縲〉ollback縲’lush縲《ync縲…lose 繧呈署萓帙＠縺ｾ縺吶・
`AkkaraDB` 縺ｨ `PackedTable<&T::id>` 縺ｯ high-level typed API 縺ｧ縺吶・++ aggregate entity 繧・BinPack 縺ｧ raw key/value row 縺ｫ螟画鋤縺励∪縺吶Ｕable key 縺ｯ 8-byte FNV-1a table prefix 縺ｧ namespace 蛻・屬縺輔ｌ縲《econdary index 縺ｯ `table_name + ":idx:" + field_name` 縺九ｉ蟆弱＞縺溷挨 prefix 繧剃ｽｿ縺・∪縺吶ょ推 row 縺ｫ縺ｯ primary key 縺ｨ縺ｯ迢ｬ遶九＠縺・stable `RowId` 繧ょ牡繧雁ｽ薙※繧峨ｌ縺ｾ縺吶・
high-level API 縺ｯ `Ref<T>` 縺ｮ lazy resolve縲’oreign-key validation縲～OnDelete` / `OnUpdate` action縲～Immutable<T>` / `Const<T>` 縺ｫ繧医ｋ immutable field縲《table row-id lookup縲～onUpdate<&Field>(...)` hook 繧よ球蠖薙＠縺ｾ縺吶・
low-level public header 縺ｫ縺ｯ縲《torage engine 縺ｮ open/close path 縺ｨ迢ｬ遶九＠縺ｦ `XorErasureCodec`縲～DualXorErasureCodec`縲～RsErasureCodec`縲～ErsCodec` 縺ｪ縺ｩ縺ｮ erasure coding utility 繧ょ性縺ｾ繧後∪縺吶・
`AKKARADB_BUILD_JNI=ON` 縺ｮ蝣ｴ蜷医゛VM layer 縺ｯ JNI 繧帝壹§縺ｦ蜷後§ native engine 縺ｫ謗･邯壹＠縺ｾ縺吶・NI bridge 縺ｯ raw operation縲《can cursor縲〈uery scan payload evaluation縲｛ption-based open縲〉ollback entry point 繧・Kotlin module 縺ｫ蜈ｬ髢九＠縺ｾ縺吶・
API error 縺ｮ蠅・阜縺ｯ `SPEC.md` 縺ｨ謠・▲縺ｦ縺・∪縺吶よ悄蠕・＆繧後ｋ縲悟ｭ伜惠縺励↑縺・阪・ `std::optional`縲～bool`縲∫ｩｺ縺ｮ range 縺ｧ陦ｨ縺励∽ｸ肴ｭ｣縺ｪ菴ｿ縺・婿繧・storage operation failure 縺ｯ exception 繧呈兜縺偵∪縺吶Ｎissing key 閾ｪ菴薙・萓句､悶〒縺ｯ縺ゅｊ縺ｾ縺帙ｓ縺後…losed engine縲（nvalid option縲》yped foreign-key target 縺ｮ谺關ｽ縲∵悴逋ｻ骭ｲ index 縺ｫ蟇ｾ縺吶ｋ `findBy()`縲｝ersisted data 縺ｮ遐ｴ謳阪！/O failure 縺ｪ縺ｩ縺ｯ standard exception 縺ｨ縺励※ surface 縺励∪縺吶・PI server 縺ｯ protocol boundary 縺ｧ縺昴ｌ繧峨ｒ catch 縺励…lient 縺ｫ縺ｯ error response / status 縺ｨ縺励※霑斐＠縺ｾ縺吶・
### AkkEngine 譛ｬ菴・
`AkkEngine` 縺ｯ coordinator 縺ｧ縺吶Ｔtorage component 繧呈園譛峨∪縺溘・謗･邯壹＠縲《ystem 蜈ｨ菴薙↓ thread-safe 縺ｪ read/write surface 繧剃ｸ弱∴縺ｾ縺吶・
荳ｻ縺ｪ雋ｬ蜍・

- `dataDir` 縺九ｉ component path 繧貞ｰ主・縺吶ｋ
- option 縺ｫ蠢懊§縺ｦ WAL縲ヾST縲。lob縲｀anifest縲〃ersionLog縲、PI縲…luster component 繧・create/open 縺吶ｋ
- top-level write 縺ｮ sequence assignment 繧・serialize 縺吶ｋ
- record 繧呈嶌縺丞燕縺ｫ large value 繧貞､夜Κ蛹悶☆繧・- 譛牙柑縺ｪ蝣ｴ蜷医・ MemTable mutation 縺ｮ蜑阪↓ WAL 縺ｨ VersionLog 縺ｫ append 縺吶ｋ
- MemTable record 繧・SST 縺ｫ flush 縺吶ｋ
- read 繧・MemTable縲ヾST縲。lobManager 縺ｫ route 縺吶ｋ
- primary 縺ｨ縺励※蜍穂ｽ懊＠縺ｦ縺・ｋ蝣ｴ蜷医・ cluster runtime 縺ｸ write/blob 繧・ship 縺吶ｋ
- component 繧貞宛蠕｡縺輔ｌ縺滄・ｺ上〒 close 縺吶ｋ

### 繝ｬ繧ｳ繝ｼ繝峨→繧ｭ繝ｼ縺ｮ繝｢繝・Ν

raw engine 縺ｯ key 繧・bytewise lexicographic order 縺ｧ豈碑ｼ・＠縺ｾ縺吶ょｿ・ｦ√↑鬆・ｺ乗ｧ繧貞ｾ励ｋ縺溘ａ縺ｮ key encoding 縺ｯ higher layer 縺ｮ雋ｬ蜍吶〒縺吶ＡPackedTable` 縺ｯ謨ｴ謨ｰ primary key 縺ｫ sortable 縺ｪ fixed-width big-endian encoding 繧剃ｽｿ縺・・縺ｧ縲》yped primary-key scan 縺ｧ繧よ焚蛟､鬆・′菫昴◆繧後∪縺吶Ｔecondary index 繧・arithmetic field 縺ｫ縺ｯ蜷後§ sortable encoding 譁ｹ驥昴ｒ菴ｿ縺・∪縺吶・
in-memory 縺ｧ縺ｯ `OwnedRecord` 縺・compact 縺ｪ 64-byte metadata object 縺ｨ縺励※ record 繧剃ｿ晄戟縺励∪縺吶・
```text
+-------------------+------------------------------------------+
| MemHdr16          | seq, key length, value length, flags     |
| key_fp64          | 64-bit key fingerprint                   |
| mini_key          | first up to 8 key bytes, little-endian   |
| SmallBuffer       | inline or arena-backed [key][value]      |
+-------------------+------------------------------------------+
```

on-disk 縺ｮ SST record 縺ｯ 32-byte 縺ｮ `SSTHdr32` 縺ｨ縲√◎繧後↓邯壹￥ key bytes / value bytes 縺ｧ縺吶Ａkey_fp64` 縺ｨ `mini_key` 縺ｯ fast reject / in-block search 逕ｨ縺ｮ hint 縺ｧ縺ゅｊ縲∵怙邨ら噪縺ｪ豁｣蠖捺ｧ縺ｯ full key compare 縺梧球菫昴＠縺ｾ縺吶・
Blob Manager 縺ｫ菫晏ｭ倥＆繧後ｋ value 縺ｯ縲〉ecord value 縺・20-byte 縺ｮ `BlobRef` 縺ｫ鄂ｮ縺肴鋤繧上ｊ縲｜lob flag 縺檎ｫ九■縺ｾ縺吶Ｓead path 縺ｧ縺ｯ縺昴ｌ繧・indirection 縺ｨ縺励※謇ｱ縺・｜lob header 縺ｨ content CRC 繧呈､懆ｨｼ縺励◆荳翫〒蜈・・ value 繧定ｿ斐＠縺ｾ縺吶・
### 繝｡繝｢繝ｪ邂｡逅・
native engine 縺ｯ hot path 縺ｮ謖吝虚繧剃ｺ域ｸｬ縺励ｄ縺吶￥縺吶ｋ縺溘ａ縲∝ｰ上＆縺乗・遉ｺ逧・↑ buffer abstraction 繧剃ｽｿ縺・∪縺吶・
`BufferArena` 縺ｯ scan 繧・API call 縺ｮ荳譎・allocation 繧呈園譛峨＠縺ｾ縺吶ＡAkkEngine::scan` 縺瑚ｿ斐☆ key/value span 縺ｯ arena 縺ｮ蟇ｿ蜻ｽ縺ｫ萓晏ｭ倥＠縺ｾ縺吶・
`OwnedBuffer` 縺ｯ I/O 蜷代￠縺ｮ aligned owning storage 縺ｧ縲～BufferView` 縺ｯ蟇ｾ蠢懊☆繧・non-owning view 縺ｧ縺吶・
`SmallBuffer` 縺ｯ `OwnedRecord` 縺ｫ蝓九ａ霎ｼ縺ｾ繧後※縺・∪縺吶ら洒縺・`[key][value]` payload 縺ｯ inline 縺ｫ蜿弱∪繧翫∝､ｧ縺阪＞ payload 縺縺代′ arena-backed storage 縺ｫ騾・′縺輔ｌ縺ｾ縺吶ゅ％繧後↓繧医ｊ common case 繧・compact 縺ｫ菫昴※縺ｾ縺吶・
### MemTable

MemTable 縺ｯ譛蛻昴・ read/write target 縺ｧ縺吶Ｄontention 繧剃ｸ九￡繧九◆繧・sharding 縺輔ｌ縺ｦ縺翫ｊ縲～RuntimeOptions::writer_threads` 縺瑚ｨｭ螳壹＆繧後※縺・ｋ蝣ｴ蜷医・ MemTable 縺ｨ WAL 縺ｮ shard count 繧・writer count 縺九ｉ蟆主・縺ｧ縺阪∪縺吶・
write sequence 縺ｯ MemTable 縺ｮ sequence model 繧帝壹§縺ｦ蜑ｲ繧雁ｽ薙※繧峨ｌ縺ｾ縺吶・
```text
reserve_seq(1) -> globally monotonic seq
last_seq()     -> read snapshot for point reads and scans
replay(seq)    -> advances sequence state during recovery
```

lookup 縺ｯ謖・ｮ・snapshot 縺ｧ蜿ｯ隕悶↑ record 繧定ｿ斐＠縺ｾ縺吶Ｕombstone 縺ｯ MemTable 蜀・〒髫阡ｽ縺帙★ caller 縺ｫ霑斐＆繧後∪縺吶ゅ％繧後・蜿､縺・SST value 繧・suppress 縺吶ｋ蠢・ｦ√′縺ゅｋ縺溘ａ縺ｧ縺吶・
flush lifecycle:

```text
shard crosses thresholdBytesPerShard
  |
  +-- shard can be sealed
  +-- engine on_flush callback receives sorted RecordView span
  +-- SSTManager::flush writes a new SST
  +-- Manifest checkpoint records the resulting state
  +-- WAL segments are pruned up to checkpoint when configured
```

### 譖ｸ縺崎ｾｼ縺ｿ蜈郁｡後Ο繧ｰ

WAL 縺ｯ crash recovery 縺ｮ縺溘ａ縺ｮ append-only mutation log 縺ｧ縺吶Ｏative v5 縺ｧ縺ｯ `{dataDir}/wal` 莉･荳九↓ segmented WAL 繧呈戟縺｡縲∝推 segment 縺ｯ CRC 菫晁ｭｷ縺輔ｌ縺・`WalSegmentHeader` 縺ｧ蟋九∪繧翫∪縺吶・
entry 縺ｮ format:

```text
[WalEntryHeader:32][key bytes][value bytes]
```

entry header 縺ｫ縺ｯ record sequence縲〔ey fingerprint縲‘ntry length縲」alue length縲〔ey length縲’lags縲√◎縺励※ header-with-zero-crc + key + value 縺ｫ蟇ｾ縺吶ｋ CRC32C 縺悟性縺ｾ繧後∪縺吶・
WAL 縺ｮ sync mode:

| 繝｢繝ｼ繝・| 諢丞袖 |
|---|---|
| `Sync` | 蜷梧悄逧・↑ durability path |
| `Async` | grouped background flush path |
| `Off` | 譏守､ｺ sync 縺ｪ縺励Ｄache / test 蜷代￠ |

startup recovery 縺ｧ縺ｯ segment header 縺ｨ entry CRC 繧呈､懆ｨｼ縺励※縺九ｉ fresh MemTable 縺ｫ驕ｩ逕ｨ縺励∪縺吶・
### Blob 繝槭ロ繝ｼ繧ｸ繝｣

Blob Manager 縺ｯ `blob.thresholdBytes` 莉･荳翫・ value 繧貞､夜Κ蛹悶＠縺ｾ縺吶よ里螳壼､縺ｯ 16 KiB 縺ｧ縺吶ゅ％繧後↓繧医ｊ MemTable record縲仝AL entry縲ヾST block 繧貞ｰ上＆縺丈ｿ昴■縺ｪ縺後ｉ縲・壼ｸｸ縺ｮ `get` API 蠖｢迥ｶ縺ｯ螟峨∴縺壹↓貂医∩縺ｾ縺吶・
write path:

```text
value size >= threshold
  |
  +-- BlobManager writes the payload to {dataDir}/blobs
  +-- optional Zstd compression is applied
  +-- Blob header and content CRC are stored
  +-- record value becomes BlobRef(blob_id, total_size, content_crc32c)
  +-- record flag includes blob
```

read path:

```text
record flag includes blob
  |
  +-- parse BlobRef
  +-- read blob file
  +-- validate header CRC and content CRC
  +-- return original value bytes
```

### SST 繝槭ロ繝ｼ繧ｸ繝｣

SST Manager 縺ｯ immutable sorted file 縺ｨ compaction 繧呈球蠖薙＠縺ｾ縺吶よ里螳壹〒縺ｯ 7 level縲～L0..L6` 繧堤ｮ｡逅・＠縺ｾ縺吶・0 縺ｯ overlapping file 繧呈戟縺｡蠕励∪縺吶′縲・ｫ倥＞ level 縺ｧ縺ｯ compaction 蠕後↓ non-overlapping 縺ｫ縺ｪ繧区Φ螳壹〒縺吶・
SST v2 file layout:

```text
[SSTFileHeaderV2:256]
[data blocks...]
[SSTBlockIndexEntryV2 array]
[key arena]
[SSTBloomHeaderV2][bloom bits]
[SSTFooterV2:48]
```

record 縺ｯ block 蜀・〒 key order 繧剃ｿ昴■縺ｾ縺吶Ｌey 縺ｯ optional 縺ｪ block compression 縺ｮ蜑阪↓ previous key 縺ｫ蟇ｾ縺吶ｋ prefix-delta encoding 縺ｫ縺ｪ縺｣縺ｦ縺・ｋ縺薙→縺後≠繧翫〉eader 縺ｯ block load 譎ゅ↓ full key 繧貞・讒区・縺励∪縺吶・
lookup 蜑肴ｮｵ:

```text
file min/max key check
  |
  +-- Bloom filter negative check
  +-- block index binary search
  +-- in-block binary search using SSTHdr32, mini_key, and full key compare
```

data block 縺ｯ raw concatenated SST record 縺・Zstd-compressed bytes 縺ｧ縺吶Ｃlock index 縺ｯ key boundary 繧呈戟縺｡縲。loom filter 縺ｯ absent key 縺ｫ蟇ｾ縺吶ｋ disk / cache work 繧呈ｸ帙ｉ縺励∪縺吶・
flush 縺ｯ sorted MemTable record 縺九ｉ譁ｰ縺励＞ SST 繧剃ｽ懊ｊ縺ｾ縺吶Ｄompaction 縺ｯ overlapping 繧ゅ＠縺上・ budget 雜・℃縺ｮ SST set 繧・lower level 縺ｫ rewrite 縺励√◎縺ｮ transition 繧・Manifest 縺ｫ險倬鹸縺励∪縺吶ＡCompactionCommit` 繧剃ｽｿ縺・％縺ｨ縺ｧ replay 譎ゅ↓ input/output file change 繧・atomic 縺ｫ驕ｩ逕ｨ縺ｧ縺阪∪縺吶・
### 繝槭ル繝輔ぉ繧ｹ繝・
Manifest 縺ｯ append-only 縺九▽ CRC 菫晁ｭｷ縺輔ｌ縺・storage lifecycle log 縺ｧ縺吶Ｎain engine manifest (`manifest.akmf`) 縺ｯ live SST file縲…ompaction transition縲…heckpoint 繧定ｿｽ霍｡縺励∝挨縺ｮ node-local cluster manifest (`cluster.akmf`) 縺ｯ cluster runtime event 繧定ｨ倬鹸縺励∪縺吶・
format:

```text
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

荳ｻ縺ｪ record type:

| Event | 蠖ｹ蜑ｲ |
|---|---|
| `SSTSeal` | 譁ｰ縺励＞ SST file 縺・seal 縺輔ｌ縺・|
| `Checkpoint` | named / unnamed checkpoint |
| `CompactionStart` | 諠・ｱ逕ｨ縺ｮ start marker |
| `CompactionCommit` | atomic 縺ｪ compaction input/output transition |
| `Truncate` | 諠・ｱ逕ｨ縺ｮ truncation marker |
| `NodeJoin`, `NodeLeave`, `PrimaryLease` | `cluster.akmf` 縺ｫ譖ｸ縺九ｌ繧・cluster runtime event |

`manifest.akmf` 縺ｮ replay 縺ｧ in-memory SST lifecycle state 繧貞・讒狗ｯ峨＠縺ｾ縺吶Ｄluster manifest 縺ｯ迴ｾ蝨ｨ縲《tartup / shutdown 繧・primary lease 縺ｮ breadcrumb 繧呈ｮ九☆ local durable event log 縺ｨ縺励※菴ｿ繧上ｌ縺ｾ縺吶Ｎalformed 縺ｾ縺溘・ CRC-invalid 縺ｪ record 縺ｯ驕ｩ逕ｨ縺輔ｌ縺ｾ縺帙ｓ縲・
### 繝舌・繧ｸ繝ｧ繝ｳ繝ｭ繧ｰ

Version Log 縺ｯ譛牙柑譎ゅ↓ per-key history 繧剃ｿ晏ｭ倥＠縲∵ｬ｡繧呈髪縺医∪縺吶・
- `getAt(key, seq)`
- `history(key)`
- `rollbackTo(seq)`
- `rollbackKey(key, seq)`

蜷・version entry 縺ｫ縺ｯ sequence縲《ource node id縲》imestamp縲’lags縲」alue bytes 縺悟・繧翫∪縺吶Ｓollback 逕ｱ譚･縺ｮ record 縺ｯ reserved rollback node id 縺ｨ rollback flag 繧剃ｽｿ縺｣縺ｦ騾壼ｸｸ write 縺ｨ蛹ｺ蛻･縺輔ｌ縺ｾ縺吶・
`SYNC` mode 縺ｯ entry 繧呈嶌縺崎ｾｼ繧薙〒 durability sync 縺励※縺九ｉ return 縺励∪縺吶ＡBATCHED_SYNC` 縺ｯ in-memory history 繧貞・縺ｫ譖ｴ譁ｰ縺励｜ackground flusher 縺ｫ batch sync 縺輔○縺ｾ縺吶ＡASYNC` 繧・background flusher 繧剃ｽｿ縺・∪縺吶′縲［ethod return 譎らせ縺ｧ batch durability 縺ｯ菫晁ｨｼ縺励∪縺帙ｓ縲Ｉeader corruption縲［alformed entry縲》runcation縲，RC mismatch 縺ｯ silent skip 縺ｧ縺ｯ縺ｪ縺・exception 縺ｫ縺ｪ繧翫∪縺吶・
Version history 縺ｯ `FAST` / `NORMAL` 縺ｧ縺ｯ default disabled縲～DURABLE` 縺ｧ縺ｯ default enabled 縺ｧ縺吶・
### API 繧ｵ繝ｼ繝舌・

`components.apiEnabled` 縺梧怏蜉ｹ縺ｪ繧峨‘ngine 縺ｯ embedded API backend 繧定ｵｷ蜍輔〒縺阪∪縺吶・
蟇ｾ蠢・backend:

| Backend | Default port | 逕ｨ騾・|
|---|---:|---|
| HTTP | 7070 | REST 鬚ｨ縺ｮ key/value operation |
| TCP | 7071 | binary AK5 request/response protocol |
| GRPC | 7072 | gRPC module 縺檎匳骭ｲ縺輔ｌ縺ｦ縺・ｋ蝣ｴ蜷医・ Protobuf/gRPC service |

binary protocol 縺ｯ `AK5Q` request frame 縺ｨ `AK5S` response frame 繧剃ｽｿ縺・∪縺吶ら樟蝨ｨ縺ｮ opcode 縺ｫ縺ｯ `Get`縲～Put`縲～Remove`縲～GetAt`縲～BatchPut`縲～BatchGet`縲～Ping`縲～Exists`縲～Count`縲～Scan`縲～History`縲～RollbackTo`縲～RollbackKey`縲～ForceSync`縲～ForceFlush`縲～Stats` 縺後≠繧翫∪縺吶・
HTTP API 縺ｯ `/v1/ping`縲～/v1/put`縲～/v1/get`縲～/v1/remove`縲～/v1/exists`縲～/v1/count`縲～/v1/scan`縲～/v1/getAt`縲～/v1/history`縲～/v1/rollbackTo`縲～/v1/rollbackKey`縲～/v1/batchPut`縲～/v1/batchGet`縲～/v1/forceSync`縲～/v1/forceFlush`縲～/v1/stats` 繧呈署萓帙＠縺ｾ縺吶Ａ/v1/scan` 縺ｨ `/v1/history` 縺ｯ `stream=1` 縺ｪ縺ｩ truthy 縺ｪ query 繧剃ｻ倥￠繧九→縲～Transfer-Encoding: chunked` 縺ｧ item 蜊倅ｽ阪・ binary frame 繧帝先ｬ｡霑斐○縺ｾ縺吶５CP 縺ｫ縺ｯ `ScanStream` / `HistoryStream` opcode縲“RPC 縺ｫ縺ｯ `ScanStream` / `HistoryStream` server-streaming RPC 縺後≠繧翫∝酔縺倡畑騾斐ｒ native transport 縺ｧ菴ｿ縺医∪縺吶・
### 繧ｯ繝ｩ繧ｹ繧ｿ螳溯｡檎ｳｻ縺ｨ TLS

cluster layer 縺ｯ 3 遞ｮ鬘槭・ deployment mode 繧呈戟縺｡縺ｾ縺吶・
| 繝｢繝ｼ繝・| 諢丞袖 |
|---|---|
| `Standalone` | local-only operation |
| `Mirror` | 蜈ｨ data-bearing node 縺ｸ write 繧・mirror 縺吶ｋ |
| `Stripe` | router policy 縺ｫ繧医ｊ key 繧・data node 縺ｫ蜑ｲ繧雁ｽ薙※繧・|

Stripe runtime 閾ｪ菴薙・逕滓・縺ｧ縺阪∪縺吶Ｓouter 縺ｯ deterministic rendezvous hashing 縺ｧ key 縺斐→縺ｫ 1 縺､縺ｮ data-bearing owner 繧帝∈縺ｳ縲・∈謚樒ｵ先棡縺ｯ `ClusterRuntime::router()` 縺九ｉ蜿門ｾ励〒縺阪∪縺吶Ｏode set 繧・placement policy 螟画峩蠕後・ ownership migration 縺ｯ縲√∪縺閾ｪ蜍輔〒縺ｯ縺ｪ縺乗・遉ｺ逧・↑驕狗畑謇矩・→縺励※謇ｱ縺・燕謠舌〒縺吶・
node role 縺ｯ `Standalone`縲～Primary`縲～Replica` 縺ｧ縺吶Ｑrimary 縺ｯ write 繧貞女縺台ｻ倥￠縺ｦ replica 縺ｸ record / blob 繧・ship 縺励〉eplica 縺ｯ engine 縺九ｉ貂｡縺輔ｌ縺・callback 縺ｧ replicated record / blob 繧帝←逕ｨ縺励∪縺吶・
`ReplicationMode` 縺ｨ runtime role 縺ｯ蛻･讎ょｿｵ縺ｧ縺吶ＡStandalone`縲～Mirror`縲～Stripe` 縺ｯ topology 繧偵～Primary` 縺ｨ `Replica` 縺ｯ process 縺ｮ襍ｷ蜍募ｽ｢諷九ｒ陦ｨ縺励∪縺吶ＡStandalone` mode 縺ｧ縺ｯ譏守､ｺ startup role 縺ｯ荳崎ｦ√〒縺吶ＡMirror` 縺ｨ `Stripe` 縺ｧ縺ｯ `ClusterRuntimeOptions::startupRole` 繧・`PRIMARY` 縺・`REPLICA` 縺ｫ險ｭ螳壹☆繧句ｿ・ｦ√′縺ゅｊ縲～AUTO` 縺ｯ runtime error 縺ｫ縺ｪ繧翫∪縺吶・
ack policy 縺ｯ `Async`縲～All`縲～Quorum` 縺ｧ縺吶・
cluster config 縺ｮ `NodeInfo.host` 縺ｯ peer 縺・dial 縺吶ｋ advertise address 縺ｧ縺吶Ｑrimary 縺ｮ replication listener 縺ｯ runtime-only 縺ｪ `repl_bind_host` 縺ｫ bind 縺励∵里螳壹・ `0.0.0.0` 縺ｧ縺吶Ｓeplication link 縺ｯ TCP 繧剃ｽｿ縺・～TransportMode::SECURE` 縺ｧ縺ｯ native secure channel 縺ｧ蛹・∩縲～TransportMode::PLAIN` 縺ｯ蜈ｨ node host 縺・loopback 縺ｾ縺溘・ private/LAN address 縺ｮ蝣ｴ蜷医・縺ｿ險ｱ蜿ｯ縺輔ｌ縺ｾ縺吶Ａlocalhost` 莉･螟悶・ hostname 縺ｯ config validation 譎ゅ↓縺ｯ non-private 縺ｨ縺ｿ縺ｪ縺励∪縺吶・
迴ｾ譎らせ縺ｮ runtime 縺ｫ縺ｯ閾ｪ蜍・primary election 縺ｯ縺ゅｊ縺ｾ縺帙ｓ縲ＡMirror` / `Stripe` 縺ｮ startup 隕∽ｻｶ:

- `PRIMARY` 縺ｯ local `selfNodeId` 縺・cluster config 縺ｫ蟄伜惠縺励…oordinator-eligible 縺ｧ縺ゅｋ縺薙→
- `REPLICA` 縺ｯ primary node id 縺ｨ蛻ｰ驕泌庄閭ｽ縺ｪ primary host / replication port 繧呈戟縺､縺薙→
- `primaryHost` 縺ｨ `primaryReplPort` 縺ｯ `primaryNodeId` 縺ｫ蟇ｾ蠢懊☆繧・config entry 縺九ｉ陬懷ｮ悟庄閭ｽ
- `REPLICA` 縺ｯ閾ｪ蛻・・霄ｫ繧・primary 縺ｨ縺励※謖・ｮ壹〒縺阪↑縺・
貅縺溘＆縺ｪ縺代ｌ縺ｰ startup 縺ｯ fail fast 縺励∪縺吶Ｔplit-brain-safe failover縲〈uorum leader election縲∬・蜍・primary re-selection 縺ｯ迴ｾ蝨ｨ scope 螟悶〒縺吶・
`PRIMARY` 縺ｨ縺励※蜍穂ｽ懊☆繧句ｴ蜷医〉untime 縺ｯ local node 縺ｮ replication port 縺ｧ `ReplicationServer` 繧帝幕縺阪∪縺吶ＡREPLICA` 縺ｮ蝣ｴ蜷医・ `ReplicationClient` 繧帝幕縺阪…onfigured primary 縺ｫ dial 縺励∪縺吶Ｓeplication ingress 縺ｯ primary 荳ｭ蠢・〒縲〉eplica 縺ｯ consumer 縺ｧ縺ゅｊ direct external write endpoint 縺ｧ縺ｯ縺ゅｊ縺ｾ縺帙ｓ縲・
`ClusterManager` 縺ｯ `{dataDir}` 莉･荳九↓ node-local 縺ｪ `cluster.akmf` 繧よ嶌縺阪∪縺吶Ｔuccessful startup 蠕後↓ `NodeJoin`縲…lean shutdown 譎ゅ↓ `NodeLeave`縲～PRIMARY` 縺ｨ縺励※襍ｷ蜍輔＠縺溘→縺阪↓ `PrimaryLease` 繧定ｨ倬鹸縺励∪縺吶・
TLS support 縺ｯ current native target 縺ｧ縺ｯ mbedTLS 縺ｧ繧ｳ繝ｳ繝代う繝ｫ縺輔ｌ縺ｦ縺・∪縺吶Ｓeplication link 縺ｯ native secure channel (`TransportMode::SECURE`) 縺ｾ縺溘・ plain TCP (`TransportMode::PLAIN`) 繧剃ｽｿ縺・∪縺吶・
---

## 繝・・繧ｿ繝輔Ο繝ｼ

### 譖ｸ縺崎ｾｼ縺ｿ繝代せ

```text
put(key, value)
  |
  +-- acquire engine write serialization
  +-- reserve seq from MemTable
  +-- if value >= blob threshold:
  |     |
  |     +-- BlobManager writes payload
  |     +-- stored value becomes 20-byte BlobRef
  |     +-- record flag includes blob
  |
  +-- append_all(seq, key, stored_value, flags)
        |
        +-- WAL append if enabled
        +-- VersionLog append if enabled
        +-- MemTable put/remove
        +-- Cluster ship_entry if enabled and this node is primary
```

sequence assignment縲仝AL append縲」ersion-log append縲｀emTable mutation縲｜lob externalization縲〉eplication shipping 縺ｯ engine write mutex 縺ｮ荳九〒鬆・ｺ丈ｻ倥￠繧峨ｌ縺ｾ縺吶ゅ％繧後↓繧医ｊ縲｝ublic method 縺御ｸｦ陦後↓蜻ｼ縺ｰ繧後※繧・mutation order 縺御ｸ雋ｫ縺励∪縺吶・
### 隱ｭ縺ｿ蜿悶ｊ繝代せ

```text
get(key)
  |
  +-- snapshot_seq = MemTable::last_seq()
  +-- MemTable::get(key, snapshot_seq)
  |     |
  |     +-- tombstone -> not found
  |     +-- normal    -> return value
  |     +-- blob      -> BlobManager::read(...)
  |
  +-- SSTManager::get(key)
        |
        +-- tombstone -> not found
        +-- normal    -> return value
        +-- blob      -> BlobManager::read(...)
        +-- optional sstPromoteReads -> insert SST hit into MemTable
```

迴ｾ蝨ｨ縺ｮ snapshot 縺ｫ縺､縺・※縺ｯ MemTable 縺悟ｸｸ縺ｫ譛蛻昴・ authority 縺ｧ縺吶４ST 縺ｯ flush 貂医∩繝・・繧ｿ縺ｮ fallback 縺ｧ縲。lob dereference 縺ｯ蜍昴▲縺・record 縺梧ｱｺ縺ｾ縺｣縺ｦ縺九ｉ陦後＞縺ｾ縺吶・
### 遽・峇襍ｰ譟ｻ繝代せ

```text
scan(start, end)
  |
  +-- caller provides BufferArena
  +-- create MemTable iterator at snapshot
  +-- create SST iterators for candidate levels/files
  +-- merge by bytewise key order
  +-- for duplicate keys, newer visible record wins
  +-- tombstones suppress older values
  +-- return ArenaGenerator<ScanRecordView>
```

霑斐＆繧後ｋ view 縺ｯ arena 縺ｮ蟇ｿ蜻ｽ縺ｫ邏蝉ｻ倥″縺ｾ縺吶ょ推 scanned key/value 繧堤峡遶九＠縺・heap object 縺ｫ驛ｽ蠎ｦ繧ｳ繝斐・縺励↑縺・◆繧√・險ｭ險医〒縺吶・
### 蝙倶ｻ倥″繝・・繝悶Ν縺ｮ繝代せ

```text
PackedTable<User, id>.put(user)
  |
  +-- encode primary row key:
  |     [table_prefix:8][encoded_pk]
  |
  +-- allocate or reuse stable RowId
  +-- maintain [pk -> rowid] and [rowid -> pk] metadata
  |
  +-- BinPack::encode(user)
  +-- engine.put(primary_key, encoded_user)
  |
  +-- for each secondary index:
        [index_prefix:8][field_len:u32le][encoded_field][encoded_pk] -> empty value
```

secondary index entry 縺ｯ non-unique 縺ｧ縺吶Ｆncoded primary key 縺ｮ suffix 縺・duplicate field value 繧貞玄蛻･縺励（ndex scan 縺九ｉ primary row 繧貞ｾｩ蜈・〒縺阪ｋ繧医≧縺ｫ縺励∪縺吶・
entity 縺ｫ `Ref<T>` field 縺後≠繧句ｴ蜷医‥irty reference 縺ｯ attached table binding 繧帝壹§縺ｦ蜈医↓譖ｸ縺九ｌ縺ｾ縺吶ゅ◎縺ｮ蠕・foreign-key validation 縺・target 縺ｮ蟄伜惠繧堤｢ｺ隱阪＠縺ｾ縺吶Ｕarget primary key 縺・`updatePrimaryKey(...)` 縺ｧ螟峨ｏ縺｣縺溘→縺阪・縲《chema 險ｭ螳壹↓蠢懊§縺ｦ `OnUpdate` action 縺・propagate / reject / null 蛹悶ｒ陦後＞縺ｾ縺吶Ｘatched field 縺悟､牙喧縺励※縺・◆蝣ｴ蜷医・縲∽ｿ晏ｭ伜燕縺ｫ local `onUpdate<&Field>(...)` hook 縺・replacement entity 繧呈嶌縺肴鋤縺医ｋ縺薙→繧ゅ〒縺阪∪縺吶・
### JNI 繧ｯ繧ｨ繝ｪ襍ｰ譟ｻ繝代せ

```text
JVM scanQuery(start, end, queryBytes, schemaBytes)
  |
  +-- JNI opens native scan cursor over [start, end)
  +-- native side decodes schema payload
  +-- native side decodes query expression payload
  +-- each row value is decoded enough to evaluate predicates
  +-- matching rows are returned to JVM RowView(ByteBufferL key, ByteBufferL value)
```

query 縺ｨ schema 縺ｮ payload integer 縺ｯ little-endian 縺ｧ縺吶Ｖnsupported operator 縺ｯ silent match 縺ｧ縺ｯ縺ｪ縺・query-evaluation error 縺ｨ縺励※謇ｱ縺・ｿ・ｦ√′縺ゅｊ縺ｾ縺吶・
---

## 繝ｪ繧ｫ繝舌Μ縺ｨ繧ｷ繝｣繝・ヨ繝繧ｦ繝ｳ

### 襍ｷ蜍墓凾繝ｪ繧ｫ繝舌Μ

`AkkEngine::open` 縺ｯ蝗ｺ螳夐・ｺ上〒 startup 縺励∪縺吶・
```text
1. Derive missing component paths from dataDir
2. Create required directories
3. Load or generate persistent node id
4. Open Manifest and prepare replay-capable state
5. Create SST Manager and recover SST state when enabled
6. Replay WAL into a fresh MemTable when enabled
7. Start WAL writer, BlobManager, VersionLog, ClusterRuntime, and API server as configured
```

recovery policy 縺ｯ component 縺斐→縺ｫ蛻・屬縺輔ｌ縺ｦ縺・∪縺吶・
- WAL recovery 縺ｯ segment header 縺ｨ entry CRC 繧呈､懆ｨｼ縺励※縺九ｉ entry 繧帝←逕ｨ縺吶ｋ
- SST reader 縺ｯ header縲’ooter縲｜lock metadata縲｜lock CRC 繧呈､懆ｨｼ縺吶ｋ
- Blob read 縺ｯ header CRC 縺ｨ original content CRC 縺ｮ荳｡譁ｹ繧呈､懆ｨｼ縺吶ｋ
- Manifest replay 縺ｯ malformed / CRC-invalid 縺ｪ record 繧帝←逕ｨ縺励↑縺・
### 髫懷ｮｳ譎ゅす繝翫Μ繧ｪ

```text
Power loss during WAL append
  -> recovery applies valid entries and stops or skips corrupted trailing data according to WAL logic

Power loss during MemTable flush
  -> WAL can rebuild records; partial SST is ignored if Manifest did not commit it

Power loss during compaction
  -> CompactionCommit is replayed atomically; without it, input files remain the live state

Power loss during blob write
  -> blob reads validate header and content CRC; unreferenced or corrupt blob payloads are not trusted
```

### 繧ｷ繝｣繝・ヨ繝繧ｦ繝ｳ

`AkkEngine::close` 縺ｯ idempotent 縺ｧ縺吶Ｄomponent 縺ｯ谺｡縺ｮ鬆・ｺ上〒 close 縺輔ｌ縺ｾ縺吶・
```text
1. API server
2. Cluster runtime
3. MemTable force flush when configured
4. SST manager shutdown
5. WAL force sync and close when configured
6. Manifest close
7. Blob manager close
8. VersionLog close
9. MemTable release
```

縺ｾ縺壼､夜Κ traffic 繧呈ｭ｢繧√√◎縺ｮ蠕・mutable state 繧・drain 縺励∵怙蠕後↓ persistent component 繧帝哩縺倥ｋ豬√ｌ縺ｧ縺吶・
---

## 荳ｦ陦梧ｧ繝｢繝・Ν

public storage component 縺ｯ public method 蠅・阜縺ｧ thread-safe 縺ｫ縺ｪ繧九ｈ縺・ｨｭ險医＆繧後※縺・∪縺吶・
| Component | 菫晁ｨｼ |
|---|---|
| `AkkEngine` | public method 縺ｯ thread-safe |
| `MemTable` | public method 縺ｯ thread-safe |
| `WalWriter` | append縲《ync縲…lose path 縺ｯ thread-safe |
| `BlobManager` | read縲『rite縲‥elete scheduling 縺ｯ thread-safe |
| `Manifest` | public method 縺ｯ thread-safe |
| `VersionLog` | public method 縺ｯ thread-safe |
| `ClusterRuntime` | start縲…lose縲《hip path 縺ｧ縺ｯ endpoint access 繧・serialize 縺吶ｋ |

top-level write 縺ｯ `AkkEngine::Impl::write_mu` 縺ｧ serialize 縺輔ｌ縺ｾ縺吶ゅ％繧後・諢丞峙逧・↑險ｭ險医〒縲《equence assignment 縺ｨ write 縺ｮ蜑ｯ菴懃畑蜈ｨ菴薙ｒ荳雋ｫ縺励◆鬆・ｺ上↓菫昴▽縺溘ａ縺ｧ縺吶・
background work 縺ｮ萓・

- WAL async flusher thread
- MemTable shard flushing
- SST compaction worker
- Manifest fast-mode flusher
- VersionLog async / batched flusher
- Blob cleanup
- API server 縺ｮ accept / connection handling
- Cluster manager 縺ｨ replication endpoint

---

## 襍ｷ蜍輔Δ繝ｼ繝・
high-level `AkkaraDB::open` 縺ｯ `StartupMode` preset 繧・`AkkEngineOptions` 縺ｫ螟画鋤縺励∪縺吶・
| 繝｢繝ｼ繝・| WAL | Blob | 繝槭ル繝輔ぉ繧ｹ繝・| SST | 繝舌・繧ｸ繝ｧ繝ｳ繝ｭ繧ｰ | close 譎ゅ・謖吝虚 | MemTable 縺励″縺・､ |
|---|---|---|---|---|---|---|---|
| `ULTRA_FAST` | disabled | disabled | disabled | disabled | disabled | force flush/sync 縺ｪ縺・| 512 MiB/shard |
| `FAST` | async | enabled | enabled | enabled | disabled | force flush/sync 縺ゅｊ | 256 MiB/shard |
| `NORMAL` | async | enabled | enabled | enabled | disabled | force flush/sync 縺ゅｊ | 64 MiB/shard |
| `DURABLE` | sync | enabled | enabled | enabled | enabled | force flush/sync 縺ゅｊ | 64 MiB/shard |

`FAST` 縺ｧ縺ｯ SST read promotion 繧よ怏蜉ｹ縺ｧ縺吶らｴｰ縺九＞ override 縺ｧ縺ｯ MemTable 縺励″縺・､縲〃ersionLog縲ヾST / blob codec縲｜lob threshold縲。loom filter density縲´0 compaction trigger縲ヾST read promotion 繧貞､画峩縺ｧ縺阪∪縺吶・
---

## SPEC.md 縺ｨ縺ｮ髢｢菫・
縺薙・譁・嶌縺ｯ `SPEC.md` 縺ｮ莉｣繧上ｊ縺ｫ縺ｪ繧九ｂ縺ｮ縺ｧ縺ｯ縺ゅｊ縺ｾ縺帙ｓ縲Ｄomponent 縺ｮ雋ｬ蜍吶→ data flow 繧呈滑謠｡縺吶ｋ縺溘ａ縺ｫ縺ｯ縺薙・譁・嶌繧剃ｽｿ縺・∵ｭ｣遒ｺ縺ｪ binary layout縲［agic number縲，RC 遽・峇縲｝rotocol frame縲…onfiguration field縲…ompatibility rule 縺ｯ `SPEC.md` 繧貞盾辣ｧ縺励※縺上□縺輔＞縲・