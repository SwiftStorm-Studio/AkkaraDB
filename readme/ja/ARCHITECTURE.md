# アーキテクチャ

AkkaraDBの内部設計と実装の詳細を説明します。

## 📚 目次

- [全体構造](#全体構造)
- [モジュール構成](#モジュール構成)
- [データフロー](#データフロー)
- [核心コンポーネント](#核心コンポーネント)
- [ディスクフォーマット](#ディスクフォーマット)
- [コンパクション戦略](#コンパクション戦略)
- [リカバリメカニズム](#リカバリメカニズム)

---

## 全体構造

AkkaraDBは、LSM-tree（Log-Structured Merge-tree）ベースのアーキテクチャを採用しています。

```
┌─────────────────────────────────────────────────────────┐
│                    Typed API (AkkDSL)                   │
│              ┌─────────────────────────────┐            │
│              │  Kotlin Compiler Plugin     │            │
│              │  (Lambda → Query AST)       │            │
│              └─────────────────────────────┘            │
├─────────────────────────────────────────────────────────┤
│                   Low-level Engine API                  │
│    put() / get() / delete() / compareAndSwap()          │
├──────────────┬──────────────┬───────────────────────────┤
│   MemTable   │     WAL      │   SSTable (LSM-tree)      │
│ (In-Memory)  │ (Durable Log)│   (On-Disk Sorted)        │
│              │              │                           │
│ • TreeMap    │ • Group      │ • L0, L1, L2, ...        │
│ • Sharded    │   Commit     │ • Bloom Filter           │
│ • Lock-free  │ • CRC32C     │ • Index Block            │
├──────────────┴──────────────┴───────────────────────────┤
│              Stripe Writer/Reader                       │
│         (k Data Lanes + m Parity Lanes)                 │
│                                                          │
│  • XOR / DualXOR / Reed-Solomon                         │
│  • Async fsync (FastMode)                               │
│  • Recovery from parity                                 │
├─────────────────────────────────────────────────────────┤
│                    Manifest                             │
│         (Append-only Event Log)                         │
│                                                          │
│  • StripeCommit / SSTSeal / Checkpoint                  │
│  • Rotation at 32MB                                     │
├─────────────────────────────────────────────────────────┤
│            Block Format (AKHdr32 + Payload)             │
│                  32 KiB Fixed Size                      │
│                                                          │
│  [payloadLen:u32][records...][padding][crc32c:u32]     │
└─────────────────────────────────────────────────────────┘
```

---

## モジュール構成

AkkaraDBは以下のモジュールで構成されています：

### akkara/common

**役割:** 基礎プリミティブとユーティリティ

**主要コンポーネント:**

- `ByteBufferL`: ゼロコピーByteBuffer拡張
- `BufferPool`: 32KiBバッファのプール管理
- `AKHdr32`: 32バイト固定ヘッダーフォーマット
- `ShortUUID`: 16バイトUUID生成
- ハッシュ関数（SipHash-2-4, MurmurHash3, xxHash64）

**依存:** なし

---

### akkara/format-api

**役割:** ブロック/レコードビューのインターフェース定義

**主要インターフェース:**

- `BlockPacker`: ブロックへのレコード詰め込み
- `BlockUnpacker`: ブロックからのレコード展開
- `RecordView`: ゼロコピーレコードビュー
- `StripeWriter`: ストライプ書き込み
- `StripeReader`: ストライプ読み込み
- `ParityCoder`: パリティ計算インターフェース

**依存:** `akkara/common`

---

### akkara/format-akk

**役割:** AKK v3フォーマットの実装

**主要コンポーネント:**

- `AkkBlockPacker`: 32KiBブロックパッカー
- `AkkBlockUnpacker`: ブロックアンパッカー
- `AkkStripeWriter`: k+m Stripeライター
- `AkkStripeReader`: Stripeリーダー
- パリティコーダー:
    - `NoParityCoder` (m=0)
    - `XorParityCoder` (m=1)
    - `DualXorParityCoder` (m=2)
    - `RSParityCoder` (m≥3, Reed-Solomon)

**依存:** `akkara/common`, `akkara/format-api`

---

### akkara/engine

**役割:** v3ストレージエンジン本体

**主要コンポーネント:**

- `AkkaraDB`: メインエンジンクラス
- `AkkDSL`: Typed API実装
- `MemTable`: インメモリKVストア
- `WalWriter` / `WalReplay`: Write-Ahead Log
- `SSTableWriter` / `SSTableReader`: SSTable I/O
- `SSTCompactor`: レベル化コンパクション
- `AkkManifest`: Manifestログ管理

**依存:** `akkara/common`, `akkara/format-api`, `akkara/format-akk`

---

### akkara/plugin

**役割:** Kotlinコンパイラプラグイン

**サブモジュール:**

- `akkara-plugin`: Gradleプラグイン
- `akkara-compiler`: IR変換プラグイン

**機能:**

- Lambda式 → Query ASTへの変換
- 型安全なクエリDSL（`&&`, `||`演算子）

**依存:** Kotlinコンパイラ

---

### akkara/akkaradb

**役割:** 統合メインモジュール（Fat JAR）

全依存関係を含むシャドウJARを生成します。

**依存:** すべてのモジュール

---

## データフロー

### 書き込みパス

```
[User: put(key, value)]
↓
┌────────────────────┐
│ 1. シーケンス採番   │  seq = mem.nextSeq()
└────────────────────┘
↓
┌────────────────────┐
│ 2. WAL書き込み      │  wal.append(op) → fsync (durable)
└────────────────────┘
↓
┌────────────────────┐
│ 3. MemTable更新     │  mem.put(key, value, seq)
└────────────────────┘
↓
┌────────────────────┐
│ 4. ACK返却         │  return seq
└────────────────────┘
↓
[バックグラウンド]
┌────────────────────┐
│ 5. MemTable flush  │  閾値超過時
│    → L0 SST        │  64MiB or 50k entries
└────────────────────┘
↓
┌────────────────────┐
│ 6. Compaction      │  L0 → L1 → L2 → ...
│    (LSM-tree)      │  maxPerLevel = 4
└────────────────────┘
↓
┌────────────────────┐
│ 7. Stripe書き込み   │  k blocks → parity計算
│    (Optional)      │  → lane files
└────────────────────┘
```

**レイテンシ内訳（P99）:**

- シーケンス採番: ≈ 1 µs
- WAL書き込み: ≈ 50-100 µs（グループコミット待ち）
- MemTable更新: ≈ 5 µs
- **合計: ≈ 60-120 µs**

---

### 読み取りパス

```
[User: get(key)]
↓
┌────────────────────┐
│ 1. MemTable検索    │  mem.get(key)
│    (Fast Path)     │
└────────────────────┘
↓ (hit)
┌────────────────────┐
│    値を返却         │  return value
└────────────────────┘
↓ (miss)
┌────────────────────┐
│ 2. SSTable検索     │  newest-first order
│    (Bloom Filter)  │  • Bloomでreject
└────────────────────┘  • Index lookup
↓                  • Block load + CRC
┌────────────────────┐
│ 3. Stripe fallback │  useStripeForRead=true時のみ
│    (Optional)      │  全Stripeをスキャン
└────────────────────┘
↓
┌────────────────────┐
│ 値 or null返却     │
└────────────────────┘
```

**レイテンシ内訳（P99）:**

- MemTableヒット: ≈ 12 µs（メモリアクセスのみ）
- SSTヒット: ≈ 30-40 µs（Block cache hot）
- Stripeフォールバック: ≈ 数ms（フルスキャン）

---

### フラッシュパス

```
[MemTable閾値超過 or flush()呼び出し]
↓
┌────────────────────┐
│ 1. MemTable seal   │  現在のMapをseal
└────────────────────┘
↓
┌────────────────────┐
│ 2. Sort & Pack     │  key順でソート
│                    │  → 32KiBブロックに詰める
└────────────────────┘
↓
┌────────────────────┐
│ 3. Index作成       │  各ブロックの先頭キー
└────────────────────┘
↓
┌────────────────────┐
│ 4. Bloom作成       │  全キーのBloomフィルター
│                    │  FP率 ≈ 1%
└────────────────────┘
↓
┌────────────────────┐
│ 5. Footer書き込み  │  AKSS footer (32B)
│                    │  • indexOff
└────────────────────┘  • bloomOff
↓                  • entries
┌────────────────────┐  • crc32c
│ 6. Manifest記録    │  SSTSeal event
└────────────────────┘
↓
┌────────────────────┐
│ 7. Compaction起動  │  L0 SST数チェック
│                    │  → 必要ならマージ
└────────────────────┘
```

---

## 核心コンポーネント

### MemTable

**設計:** ロックシャード化TreeMap

```kotlin
class MemTable(
    shardCount: Int = 4 - 8,
    thresholdBytesPerShard: Long = 64 MB / shardCount
) {
    private val shards: Array<Shard>
    private val globalSeq: AtomicLong

    data class Shard(
        val map: TreeMap<ByteArray, MemRecord>,
        val lock: ReentrantReadWriteLock,
        val sizeBytes: AtomicLong
    )
}
```

**特徴:**

- キーハッシュによるシャード分散
- 読み取りはread lock、書き込みはwrite lock
- 各シャードで閾値監視
- Seal & Swap方式でフラッシュ

**置換ルール（shouldReplace）:**

```kotlin
fun shouldReplace(old: MemRecord, new: MemRecord): Boolean {
    return when {
        new.seq > old.seq -> true               // higher seq wins
        new.seq < old.seq -> false
        new.tombstone && !old.tombstone -> true // tie: tombstone wins
        else -> false
    }
}
```

---

### WAL (Write-Ahead Log)

**フォーマット:** v3フレーミング

```
[Frame] = [length:u32][payload][crc32c:u32]
```

**payload:**

```
AKHdr32 (32B) + key + value
```

**グループコミット:**

```kotlin
class WalWriter(
    groupN: Int = 64,           // N個まとめる
    groupTmicros: Long = 1_000  // または T µs経過
) {
    private val pending: ConcurrentLinkedQueue<WalOp>
    private val flusher: Thread
}
```

**動作:**

1. `append(op)` → pendingキューに追加
2. フラッシャースレッドが周期的にチェック
3. `N個貯まる` OR `T µs経過` → バッチfsync
4. FastMode: `force(false)` (fdatasync)
5. DurableMode: `force(true)` (fsync)

**リカバリ:**

```kotlin
fun replay(walPath: Path, mem: MemTable): Result {
    // 1. WALファイルをmmap
    // 2. フレームを順次読み取り
    // 3. AKHdr32解析 → MemTableに適用
    // 4. 不完全フレームでストップ（切り詰め許容）
}
```

---

### SSTable

**ファイル構造:**

```
[Block 0: Data]
[Block 1: Data]
...
[Block N-1: Data]
[Index Block]
[Bloom Filter]
[Footer: AKSS 32B]
```

**Block（32 KiB）:**

```
[0..3]     payloadLen (u32, LE)
[4..N)     payload = repeated { AKHdr32 + key + value }
[N..-5]    zero padding
[-4..-1]   crc32c (u32, LE)
```

**AKHdr32（32 Bytes, LE）:**

```
[0..1]     kLen (u16)
[2..5]     vLen (u32)
[6..13]    seq (u64)
[14]       flags (u8)  // TOMBSTONE = 0x01
[15]       pad0 (u8)
[16..23]   keyFP64 (u64)  // SipHash-2-4
[24..31]   miniKey (u64)  // 先頭8バイト（LE）
```

**Index Block:**

```
repeated {
    blockOffset (u64)
    firstKey32 (32B fixed, zero-padded)
}
```

**Bloom Filter:**

- ビット配列（エントリ数 × 10 bits）
- ハッシュ関数: 7個
- False Positive率: ≈ 1%

**AKSS Footer（32 Bytes）:**

```
[0..3]     magic 'AKSS' (u32)
[4]        version (u8) = 1
[5..7]     padding
[8..15]    indexOff (u64)
[16..23]   bloomOff (u64)
[24..27]   entries (u32)
[28..31]   crc32c (u32)  // over [0..fileSize-4)
```

---

### Stripe

**レーン構成:**

```
k data lanes + m parity lanes

例: k=4, m=2
lanes/
├── data_0
├── data_1
├── data_2
├── data_3
├── parity_0
└── parity_1
```

**書き込みフロー:**

```
[k個のブロック貯まる]
    ↓
[パリティ計算: m個のparityブロック]
    ↓
[全レーンに同時書き込み]
    data_0[stripe_i]   = block_0
    data_1[stripe_i]   = block_1
    ...
    parity_0[stripe_i] = XOR(block_0..block_{k-1})
    ↓
[グループコミットスケジュール]
    N stripes OR T µs → fsync
```

**パリティコーダー:**

#### XOR (m=1)

```
parity = block_0 ⊕ block_1 ⊕ ... ⊕ block_{k-1}
```

1レーン故障まで復旧可能。

#### DualXOR (m=2)

```
parity_0 = block_0 ⊕ block_1 ⊕ block_2 ⊕ block_3
parity_1 = (1*block_0) ⊕ (2*block_1) ⊕ (3*block_2) ⊕ (4*block_3)
```

2レーン故障まで復旧可能。

#### Reed-Solomon (m≥3)

Galois Field GF(2^8)上の演算。
最大mレーン故障まで復旧可能。

**リカバリ:**

```kotlin
fun recover(): RecoveryResult {
    // 1. 各レーンの最後のstripe indexを確認
    // 2. 不一致があれば切り詰め検出
    // 3. parityから欠損レーンを復元
    // 4. 全レーンをexactSizeに切り詰め
}
```

---

### Manifest

**役割:** システム状態の追記専用ログ

**イベント種別:**

```kotlin
sealed class ManifestEvent {
    data class StripeCommit(val stripe: Long)
    data class SSTSeal(val level: Int, val file: String, val entries: Long, ...)
    data class CompactionStart(val level: Int, val inputs: List<String>)
    data class CompactionEnd(val level: Int, val output: String, ...)
    data class SSTDelete(val file: String)
    data class Checkpoint(val name: String, val stripe: Long, val lastSeq: Long)
    data class Truncate(val reason: String)
    data class FormatBump(val oldVer: Int, val newVer: Int)
}
```

**フォーマット:**

```
[length:u32][json_payload][crc32c:u32]
```

**Rotation:**

- ファイルサイズが32MBを超えると新ファイルに切り替え
- `manifest.akman.0`, `manifest.akman.1`, ...

**リカバリ時:**

```kotlin
fun replay(): State {
    // 1. 全manifestファイルを読み込み
    // 2. イベントを順次適用してメモリ状態を復元
    // 3. 最後のCheckpointを特定
    // 4. ライブSSTファイル一覧を構築
}
```

---

## ディスクフォーマット

### ディレクトリ構造

```
baseDir/
├── wal.akwal                # Write-Ahead Log
├── manifest.akman.0         # Manifestログ
├── manifest.akman.1
├── sst/                     # SSTableディレクトリ
│   ├── L0/
│   │   ├── sst_001.sst
│   │   └── sst_002.sst
│   ├── L1/
│   │   └── sst_003.sst
│   └── L2/
│       └── sst_004.sst
└── lanes/                   # Stripeレーンディレクトリ
    ├── data_0
    ├── data_1
    ├── data_2
    ├── data_3
    ├── parity_0
    └── parity_1
```

### バージョン管理

**Magic番号:**

- WAL: なし（v3フレーミング）
- SST: `AKSS` (0x414B5353)
- Manifest: JSONベース

**フォーマットバージョン:**

- v3: 現行フォーマット
- 将来のv4: FormatBumpイベントで移行

**後方互換性:**

- v3 → v2: オフラインコンパクタで変換
- v4 → v3: FormatBumpイベントで段階的移行

---

## コンパクション戦略

### レベル化コンパクション

```
L0: 最大4 SST (書き込み直後)
 ↓ compact
L1: 最大4 SST (10× L0のサイズ)
 ↓ compact
L2: 最大4 SST (10× L1のサイズ)
 ↓ ...
```

**トリガー条件:**

```kotlin
fun shouldCompact(level: Int): Boolean {
    val files = listSstFiles(level)
    return files.size > maxPerLevel  // デフォルト4
}
```

**コンパクションアルゴリズム:**

```kotlin
fun compactLevel(level: Int) {
    // 1. 現在レベルの全SSTを取得
    val currentLevelFiles = listSstFiles(level)

    // 2. 次レベルの全SSTを取得
    val nextLevelFiles = listSstFiles(level + 1)

    // 3. K-way mergeでマージ
    val output = nextLevelPath.resolve(newFileName())
    val (entries, firstKey, lastKey) =
        mergeInto(currentLevelFiles + nextLevelFiles, output, isBottomLevel)

    // 4. 入力ファイルを削除
    currentLevelFiles.forEach { delete(it) }
    nextLevelFiles.forEach { delete(it) }

    // 5. Manifestに記録
    manifest.compactionEnd(level, output, entries, firstKey, lastKey)
}
```

**Tombstone GC:**

- ボトムレベル（最下層）でのみGC
- TTL: デフォルト24時間
- 条件: `isBottomLevel && (now - tombstoneTime) > TTL`

---

## リカバリメカニズム

### 起動時リカバリフロー

```
[AkkaraDB.open()]
    ↓
┌─────────────────────────┐
│ 1. Manifest読み込み     │
│    • ライブSST一覧      │
│    • 最後のCheckpoint   │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│ 2. WAL再生              │
│    • MemTableに適用     │
│    • lastSeq復元        │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│ 3. Stripe検証           │
│    • 各レーンの長さ確認 │
│    • 不一致→切り詰め    │
│    • Parity復元         │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│ 4. SSTableリーダー構築  │
│    • 全レベルをスキャン │
│    • newest-first順     │
└─────────────────────────┘
    ↓
[起動完了]
```

### WALリカバリ

**切り詰め許容:**

```kotlin
fun readOne(buf: ByteBuffer): ByteArray? {
    if (buf.remaining < 8) return null  // 不完全フレーム

    val length = buf.getInt()
    if (buf.remaining < length + 4) return null  // payload不完全

    val payload = ByteArray(length)
    buf.get(payload)

    val crc = buf.getInt()
    val computed = CRC32C.compute(payload)

    if (crc != computed) return null  // CRC不一致→停止

    return payload
}
```

**適用:**

```kotlin
for (payload in walFrames) {
    val hdr = AKHdr32.read(payload)
    val key = payload.slice(32, hdr.kLen)
    val value = payload.slice(32 + hdr.kLen, hdr.vLen)

    if (hdr.isTombstone) {
        mem.delete(key, hdr.seq)
    } else {
        mem.put(key, value, hdr.seq)
    }
}
```

### Stripeリカバリ

**検証:**

```kotlin
fun verifyStripes(): RecoveryResult {
    val dataLengths = dataChannels.map { it.size() / blockSize }
    val parityLengths = parityChannels.map { it.size() / blockSize }

    val allLengths = dataLengths + parityLengths
    val maxStripes = allLengths.maxOrNull() ?: 0
    val minStripes = allLengths.minOrNull() ?: 0

    if (maxStripes != minStripes) {
        // 不一致検出 → 切り詰め必要
        return RecoveryResult(
            lastSealed = minStripes - 1,
            lastDurable = minStripes - 1,
            truncatedTail = true
        )
    }

    return RecoveryResult(lastSealed = maxStripes - 1, lastDurable = maxStripes - 1)
}
```

**パリティ復元:**

```kotlin
fun reconstructLane(missingLaneIdx: Int, stripeIdx: Long): ByteBuffer {
    val blocks = mutableListOf<ByteBuffer>()

    // 生き残っているk-1個のデータレーンを読み込み
    for (i in 0 until k) {
        if (i != missingLaneIdx) {
            blocks.add(readDataLane(i, stripeIdx))
        }
    }

    // パリティレーンを読み込み
    for (i in 0 until m) {
        blocks.add(readParityLane(i, stripeIdx))
    }

    // パリティコーダーで復元
    return parityCoder.reconstruct(blocks, missingLaneIdx)
}
```

### クラッシュセーフ性保証

**不変条件:**

```
last_durable_WAL ≤ last_sealed_manifest
```

**保証メカニズム:**

1. WALが先に書き込まれる（write-ahead）
2. Manifestは書き込み完了後に記録
3. リカバリ時はManifestの境界までWAL再生

**例（クラッシュシナリオ）:**

```
t0: WAL[seq=100] written & fsynced
t1: MemTable[seq=100] updated
t2: [CRASH] ← Manifestにはまだ記録されていない
---
Recovery:
t3: Manifest読み込み → lastSeq=99
t4: WAL再生 → seq=100を再適用
t5: 起動完了 → データ整合性保証
```

---

次へ: [ベンチマーク](./BENCHMARKS.md) | [API リファレンス](./API_REFERENCE.md)

[概要に戻る](./ABOUT.md)

---