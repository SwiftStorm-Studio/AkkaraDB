# API リファレンス

AkkaraDBの全API仕様を説明します。

## 📚 目次

- [Typed API (AkkDSL)](#typed-api-akkdsl)
- [Low-level API](#low-level-api)
- [起動モード](#起動モード)
- [Options設定](#options設定)
- [データ型](#データ型)
- [エラーハンドリング](#エラーハンドリング)

---

## Typed API (AkkDSL)

型安全なKotlin DSL APIです。Kotlinコンパイラプラグインにより、データクラスを直接キーバリューとして扱えます。

### データベースを開く

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.StartupMode
import java.nio.file.Paths

data class User(val name: String, val age: Int)

val base = Paths.get("./data/akkdb")
val db = AkkDSL.open<User>(base, StartupMode.NORMAL)
```

**シグネチャ:**

```kotlin
fun <T> open(
    baseDir: Path,
    mode: StartupMode = StartupMode.NORMAL
): TypedTable<T>
```

### 基本操作

#### put - データの書き込み

```kotlin
val seq: Long = db.put(namespace: String, id: String, value: T)
```

**例:**

```kotlin
val seq = users.put("user", "12345", User("太郎", 25))
println("書き込み完了: seq=$seq")
```

**戻り値:** グローバルシーケンス番号（u64）

---

#### get - データの読み取り

```kotlin
val value: T? = db.get(namespace: String, id: String)
```

**例:**

```kotlin
val user = users.get("user", "12345")
if (user != null) {
    println("Found: $user")
} else {
    println("Not found")
}
```

**戻り値:** 値が存在する場合は`T`、存在しない場合やtombstoneの場合は`null`

---

#### delete - データの削除

```kotlin
val seq: Long = db.delete(namespace: String, id: String)
```

**例:**

```kotlin
val seq = users.delete("user", "12345")
println("削除完了: seq=$seq")
```

**戻り値:** 削除時のシーケンス番号

---

#### compareAndSwap - 条件付き更新

```kotlin
val success: Boolean = db.compareAndSwap(
    namespace: String,
    id: String,
    expectedSeq: Long,
    newValue: T?
)
```

**例:**

```kotlin
val seq1 = users.put("user", "12345", User("太郎", 25))

// seq1が一致する場合のみ更新
val success = users.compareAndSwap(
    "user", "12345",
    expectedSeq = seq1,
    newValue = User("太郎", 26)
)

if (success) {
    println("更新成功")
} else {
    println("更新失敗（競合が発生）")
}

// 削除する場合はnewValueにnullを指定
users.compareAndSwap("user", "12345", expectedSeq = seq1, newValue = null)
```

**戻り値:** 更新成功時は`true`、expectedSeqが一致しない場合は`false`

---

#### range - 範囲検索

```kotlin
val results: Sequence<Triple<String, String, T>> = db.range(
    namespace: String,
    startId: String,
    endId: String
)
```

**例:**

```kotlin
// user:00000000 から user:00000099 の範囲
for ((ns, id, user) in users.range("user", "00000000", "00000099")) {
    println("$ns:$id -> $user")
}

// 全範囲を検索（危険、大量データの場合は注意）
for ((ns, id, user) in users.range("user", "", "\uFFFF")) {
    println("$ns:$id -> $user")
}
```

**戻り値:** `Triple<namespace, id, value>`のSequence

**注意:**

- `endId`は排他的（含まない）
- 大量データの場合はメモリに注意
- tombstoneは自動的にスキップされる

---

#### close - データベースを閉じる

```kotlin
db.close()
```

全ての変更をフラッシュし、リソースを解放します。

---

## Low-level API

`ByteBufferL`を使った直接操作APIです。シリアライゼーションを自分で管理する必要があります。

### データベースを開く

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import java.nio.file.Paths

val db = AkkaraDB.open(
    AkkaraDB.Options(baseDir = Paths.get("./data/akkdb"))
)
```

### 基本操作

#### put - キーバリューの書き込み

```kotlin
fun put(key: ByteBufferL, value: ByteBufferL): Long
```

**例:**

```kotlin
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.charset.StandardCharsets

val key = ByteBufferL.wrap(
    StandardCharsets.UTF_8.encode("mykey")
).position(0)

val value = ByteBufferL.wrap(
    StandardCharsets.UTF_8.encode("myvalue")
).position(0)

val seq = db.put(key, value)
```

**戻り値:** グローバルシーケンス番号

---

#### get - キーから値を読み取る

```kotlin
fun get(key: ByteBufferL): ByteBufferL?
```

**例:**

```kotlin
val result = db.get(key)
if (result != null) {
    val str = StandardCharsets.UTF_8.decode(result.rawDuplicate()).toString()
    println("Value: $str")
}
```

**戻り値:** 値が存在する場合は`ByteBufferL`、存在しない場合は`null`

---

#### delete - キーの削除

```kotlin
fun delete(key: ByteBufferL): Long
```

**例:**

```kotlin
val seq = db.delete(key)
```

**戻り値:** 削除時のシーケンス番号

---

#### compareAndSwap - 条件付き更新

```kotlin
fun compareAndSwap(
    key: ByteBufferL,
    expectedSeq: Long,
    newValue: ByteBufferL?
): Boolean
```

**例:**

```kotlin
val seq1 = db.put(key, value)
val success = db.compareAndSwap(key, expectedSeq = seq1, newValue = newValue)
```

**戻り値:** 更新成功時は`true`、失敗時は`false`

---

#### range - 範囲検索

```kotlin
fun range(
    start: ByteBufferL,
    end: ByteBufferL
): Sequence<MemRecord>
```

**例:**

```kotlin
val startKey = ByteBufferL.wrap(StandardCharsets.UTF_8.encode("key:0000")).position(0)
val endKey = ByteBufferL.wrap(StandardCharsets.UTF_8.encode("key:9999")).position(0)

for (record in db.range(startKey, endKey)) {
    println("Key: ${record.key}, Value: ${record.value}, Seq: ${record.seq}")
}
```

**戻り値:** `MemRecord`のSequence

---

#### flush - 強制フラッシュ

```kotlin
fun flush()
```

MemTableをSSTableに書き出し、ストライプをシールし、Manifestにチェックポイントを記録します。

**例:**

```kotlin
db.put(key, value)
db.flush() // 明示的にフラッシュ
```

---

#### close - データベースを閉じる

```kotlin
fun close()
```

`flush()`を呼び出した後、全リソースを解放します。

---

## 起動モード

Typed APIでは、用途に応じて起動モードを選択できます。

### StartupMode.NORMAL（推奨）

バランス型の設定。ほとんどのユースケースに適しています。

```kotlin
val db = AkkDSL.open<User>(base, StartupMode.NORMAL)
```

**設定:**

- `walGroupN = 128`
- `walGroupMicros = 5_000`
- `walFastMode = true`
- `stripeFastMode = true`

**特性:**

- 書き込みP99: ≈ 100-200 µs
- 耐久性: 高い（WALグループコミット）

---

### StartupMode.FAST

書き込み速度優先。耐久性は若干低下します。

```kotlin
val db = AkkDSL.open<User>(base, StartupMode.FAST)
```

**設定:**

- `walGroupN = 512`
- `walGroupMicros = 50_000`
- `walFastMode = true`
- `stripeFastMode = true`

**特性:**

- 書き込みP99: ≈ 60 µs
- 耐久性: 中程度（最大50ms遅延）

---

### StartupMode.DURABLE

耐久性優先。書き込み速度は低下します。

```kotlin
val db = AkkDSL.open<User>(base, StartupMode.DURABLE)
```

**設定:**

- `walGroupN = 32`
- `walGroupMicros = 500`
- `walFastMode = false`
- `stripeFastMode = false`

**特性:**

- 書き込みP99: ≈ 500-1000 µs
- 耐久性: 最高（即座にfsync）

---

### StartupMode.ULTRA_FAST

テスト用。fsyncを最小化します。**本番環境では使用しないことを推奨します。**

```kotlin
val db = AkkDSL.open<User>(base, StartupMode.ULTRA_FAST)
```

**設定:**

- `walGroupN = 1024`
- `walGroupMicros = 100_000`
- `walFastMode = true`
- `stripeFastMode = true`

**特性:**

- 書き込みP99: ≈ 20-40 µs
- 耐久性: 低い（クラッシュ時にデータ損失の可能性）

---

## Options設定

Low-level APIでは、詳細なチューニングが可能です。

### AkkaraDB.Options

```kotlin
data class Options(
    val baseDir: Path,                          // データディレクトリ
    val k: Int = 4,                             // データレーン数
    val m: Int = 2,                             // パリティレーン数
    val flushPolicy: FlushPolicy = FlushPolicy(
        maxBlocks = 32,
        maxMicros = 500
    ),
    val walFastMode: Boolean = true,            // WAL高速モード
    val stripeFastMode: Boolean = true,         // Stripe高速モード
    val walGroupN: Int = 64,                    // WALグループコミット数
    val walGroupMicros: Long = 1_000,           // WALグループコミット時間(µs)
    val parityCoder: ParityCoder? = null,       // パリティコーダー（null=自動選択）
    val durableCas: Boolean = true,             // CAS時にWALに書き込むか
    val useStripeForRead: Boolean = false       // 読み取り時にStripeを使うか
)
```

### パラメータ詳細

#### baseDir

データベースのルートディレクトリ。以下のファイル/ディレクトリが作成されます:

- `wal.akwal` - Write-Ahead Log
- `manifest.akman` - Manifestファイル
- `sst/` - SSTableファイル（L0, L1, ...）
- `lanes/` - Stripeレーンファイル（data_0, data_1, ..., parity_0, ...）

#### k（データレーン数）

Stripeのデータレーン数。通常は4が推奨。

**調整指針:**

- `k = 4`: バランス型（推奨）
- `k = 8`: 高スループット（書き込み帯域が広い）
- `k = 2`: 低レイテンシ優先

#### m（パリティレーン数）

Stripeのパリティレーン数。冗長性のレベルを決定します。

**設定:**

- `m = 0`: パリティなし（冗長性なし）
- `m = 1`: XORパリティ（1レーン故障まで復旧可能）
- `m = 2`: DualXORパリティ（2レーン故障まで復旧可能）
- `m ≥ 3`: Reed-Solomonパリティ（mレーン故障まで復旧可能）

**推奨:**

- 通常: `m = 2`（DualXOR）
- 高信頼性: `m = 3`以上（Reed-Solomon）
- テスト: `m = 0`（パリティなし）

#### walGroupN

WALのグループコミット数。この数のエントリが貯まるか、`walGroupMicros`が経過するとfsyncが発行されます。

**調整指針:**

- 小さい値（32-64）: 低レイテンシ、低スループット
- 中間値（128-256）: バランス型
- 大きい値（512-1024）: 高スループット、高レイテンシ

#### walGroupMicros

WALのグループコミット時間（マイクロ秒）。

**調整指針:**

- `500-1_000 µs`: 低レイテンシ
- `5_000-10_000 µs`: バランス型
- `50_000-100_000 µs`: 高スループット（最大遅延50-100ms）

#### walFastMode

`true`の場合、`force(false)`（fdatasync相当）を使用。`false`の場合、`force(true)`（fsync相当）を使用。

**推奨:** ほとんどの場合`true`

#### stripeFastMode

`true`の場合、Stripeのfsyncを非同期化。

**推奨:** ほとんどの場合`true`

#### durableCas

`true`の場合、CAS操作時にもWALに書き込む。

**推奨:** `true`（リカバリ時の整合性保証）

#### useStripeForRead

`true`の場合、MemTableとSSTで見つからない場合にStripeからフォールバック読み取りを行う。

**推奨:** `false`（パフォーマンス優先）。デバッグ時のみ`true`。

---

## データ型

### MemRecord

Low-level APIの`range()`で返されるレコード型。

```kotlin
data class MemRecord(
    val key: ByteBufferL,           // キー
    val value: ByteBufferL,         // 値（tombstoneの場合はEMPTY）
    val seq: Long,                  // シーケンス番号
    val flags: Byte,                // フラグ（TOMBSTONE = 0x01）
    val keyHash: Int,               // キーハッシュ
    val approxSizeBytes: Int        // 概算メモリサイズ
) {
    val tombstone: Boolean
        get() = (flags.toInt() and 0x01) != 0
}
```

### ByteBufferL

AkkaraDB専用のByteBuffer拡張型。ゼロコピー操作をサポートします。

**主なメソッド:**

```kotlin
// 生成
fun allocate(size: Int, direct: Boolean = true): ByteBufferL
fun wrap(buffer: ByteBuffer): ByteBufferL

// 位置操作
fun position(newPosition: Int): ByteBufferL
fun limit(newLimit: Int): ByteBufferL

// 読み取り（Little Endian）
val i8: Int          // u8として読み取り（0-255）
val i16: Int         // i16として読み取り
val i32: Int         // i32として読み取り
val i64: Long        // i64として読み取り

// 書き込み（Little Endian）
fun put(src: ByteBufferL, length: Int)
fun putBytes(bytes: ByteArray)

// ユーティリティ
fun duplicate(): ByteBufferL
fun asReadOnlyDuplicate(): ByteBufferL
fun rawDuplicate(): ByteBuffer  // 元のByteBufferを取得
```

**使用例:**

```kotlin
val buf = ByteBufferL.allocate(1024)
buf.at(0).i32 = 42          // オフセット0にi32を書き込み
val value = buf.at(0).i32   // オフセット0からi32を読み取り
```

---

## エラーハンドリング

### 例外型

AkkaraDBは以下の例外をスローする可能性があります:

#### CorruptedBlockException

ブロックのCRC検証に失敗した場合。

```kotlin
try {
    val value = db.get(key)
} catch (e: CorruptedBlockException) {
    logger.error("データ破損検出: ${e.message}")
    // リカバリ処理
}
```

#### IO_CORRUPT

I/O操作中にデータ破損が検出された場合。

#### PARITY_MISMATCH

パリティ検証に失敗した場合。

#### WAL_TRUNCATED

WALファイルが切り詰められている場合（通常はリカバリ中）。

#### FORMAT_UNSUPPORTED

サポートされていないフォーマットバージョンの場合。

### リカバリ戦略

データベースが異常終了した場合、次回起動時に自動的にリカバリが実行されます:

1. Manifestを読み込み、最後の一貫性境界を特定
2. WALを再生してMemTableを再構築
3. Stripeの検証とパリティによる復旧（必要に応じて）

**手動リカバリが必要な場合:**

```kotlin
val db = AkkaraDB.open(
    AkkaraDB.Options(
        baseDir = base,
        useStripeForRead = true  // Stripeフォールバックを有効化
    )
)
```

---

次へ: [アーキテクチャ](./ARCHITECTURE.md) | [ベンチマーク](./BENCHMARKS.md)

[概要に戻る](./ABOUT.md)

---