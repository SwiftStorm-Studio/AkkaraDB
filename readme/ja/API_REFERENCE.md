# API リファレンス

AkkaraDBの全API仕様を説明します。

## 📚 目次

- [Typed API (AkkDSL)](#typed-api-akkdsl)
- [Low-level API](#low-level-api)
- [起動モード](#起動モード)
- [Options設定](#options設定)
- [データ型](#データ型)
- [Query API](#query-api)

---

## Typed API (AkkDSL)

型安全なKotlin DSL APIです。Kotlinコンパイラプラグインにより、データクラスを直接キーバリューとして扱えます。

### データベースを開く

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.StartupMode
import dev.swiftstorm.akkaradb.common.ShortUUID
import dev.swiftstorm.akkaradb.engine.Id
import java.nio.file.Paths

data class User(
    @Id val id: ShortUUID,
    val name: String,
    val age: Int
)

val base = Paths.get("./data/akkdb")
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.NORMAL)
```

**シグネチャ:**

```kotlin
inline fun <reified T : Any, reified ID : Any> open(
    baseDir: Path,
    configure: AkkDSLCfgBuilder.() -> Unit = {}
): PackedTable<T, ID>
```

```kotlin
inline fun <reified T : Any, reified ID : Any> open(
    baseDir: Path,
    mode: StartupMode,
    noinline customize: AkkDSLCfgBuilder.() -> Unit = {}
): PackedTable<T, ID>
```

```kotlin
inline fun <reified T : Any, reified ID : Any> open(
    cfg: AkkDSLCfg
): PackedTable<T, ID>
```

### 基本操作

#### put - データの書き込み

```kotlin
db.put(id: ID, entity: T)
// or
db.put(entity: T)
```

**例:**

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.NORMAL)
val id = ShortUUID.generate()
val user = User(id, "太郎", 25)

// IDと値を指定して書き込み
db.put(id, user)

// @Idアノテーションが付いたプロパティから自動でIDを抽出
db.put(user)
```

**戻り値:** なし（void）

**注意:**

- Low-level APIの`put()`はグローバルシーケンス番号を返しますが、Typed APIの`put()`は戻り値がありません
- シーケンス番号が必要な場合は`db.akkdb.lastSeq()`でアクセス可能

---

#### get - データの読み取り

```kotlin
val value: T? = db.get(id: ID)
```

**例:**

```kotlin
val id = ShortUUID.generate()
val user = db.get(id)
if (user != null) {
    println("ユーザー発見: $user")
} else {
    println("ユーザーが存在しないか削除されています")
}
```

**戻り値:** 値が存在する場合は`T`、存在しない場合やtombstoneの場合は`null`

---

#### delete - データの削除

```kotlin
db.delete(id: ID)
```

**例:**

```kotlin
val id = ShortUUID.generate()
db.delete(id)
println("削除完了")
```

**戻り値:** なし（void）

---

#### upsert - データの更新または挿入

```kotlin
val entity: T = db.upsert(id: ID, init: T.() -> Unit)
```

**例:**

```kotlin
// IDが存在すれば更新、存在しなければ新規作成
val user = db.upsert(id) {
    name = "次郎"
    age = 30
}
println("Upsert完了: $user")
```

**戻り値:** 更新または作成されたエンティティ

**注意:**

- エンティティが存在しない場合、引数なしコンストラクタが必要
- `init`ラムダで既存のプロパティを変更

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
println("書き込み完了: seq=$seq")
```

**戻り値:** グローバルシーケンス番号

**内部動作:**

1. `mem.nextSeq()`で新しいシーケンス番号を取得
2. WALに`WalOp.Add`を書き込み（durable before apply）
3. MemTableに書き込み
4. シーケンス番号を返す

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

**戻り値:** 値が存在する場合は`ByteBufferL`、存在しない場合やtombstoneの場合は`null`

**内部動作:**

1. MemTableから検索（高速パス）
2. 見つからなければSSTableReader群を新しい順に検索
3. `useStripeForRead=true`の場合、Stripeからフォールバック検索

---

#### delete - キーの削除

```kotlin
fun delete(key: ByteBufferL): Long
```

**例:**

```kotlin
val seq = db.delete(key)
println("削除完了: seq=$seq")
```

**戻り値:** 削除時のシーケンス番号

**内部動作:**

1. `mem.nextSeq()`で新しいシーケンス番号を取得
2. WALに`WalOp.Delete`（tombstoneフラグ付き）を書き込み
3. MemTableにtombstoneを挿入

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

// seq1が一致する場合のみ更新
val success = db.compareAndSwap(key, expectedSeq = seq1, newValue = newValue)

if (success) {
    println("更新成功")
} else {
    println("更新失敗（競合が発生）")
}

// 削除する場合はnewValueにnullを指定
db.compareAndSwap(key, expectedSeq = seq1, newValue = null)
```

**戻り値:** 更新成功時は`true`、expectedSeqが一致しない場合は`false`

**内部動作:**

1. MemTableで`compareAndSwap()`を実行
2. 成功かつ`durableCas=true`の場合、WALに書き込み（idempotent）
3. 成功/失敗を返す

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

**内部動作:**

1. MemTableとSSTableReader群から各イテレータを取得
2. K-wayマージでキー順にソート
3. 同じキーが複数ある場合は最新（seq最大）を優先
4. tombstoneはスキップ

**注意:**

- `end`は排他的（含まない）
- 大量データの場合はメモリに注意

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

## カスタムシリアライゼーション

### 概要

AkkaraDBは標準的なKotlinデータクラスを自動的にシリアライズしますが、外部ライブラリの型や特殊なシリアライズロジックが必要な型には、カスタム`TypeAdapter`を登録できます。

### TypeAdapter インターフェース

```kotlin
interface TypeAdapter<T> {
    /** シリアライズ後の推定サイズ（バイト）。上限値を返すべき。 */
    fun estimateSize(value: T): Int

    /** 値をバッファにシリアライズ。buffer.positionを進める。 */
    fun write(value: T, buffer: ByteBufferL)

    /** バッファから値をデシリアライズ。buffer.positionを進める。 */
    fun read(buffer: ByteBufferL): T

    /** ディープコピー（デフォルト実装あり：encode→decode） */
    fun copy(value: T): T
}
```

### アダプターの登録

```kotlin
import dev.swiftstorm.akkaradb.common.binpack.AdapterRegistry

// 方法1: reified型パラメータで登録（推奨）
AdapterRegistry.register<Component>(componentAdapter)

// 方法2: KClassで登録
AdapterRegistry.registerAdapter(Component::class, componentAdapter)

// 方法3: KTypeで登録（高度な用途）
AdapterRegistry.registerAdapter(typeOf<Component>(), componentAdapter)
```

### 実装例：外部ライブラリの型

```kotlin
import dev.swiftstorm.akkaradb.common.ByteBufferL
import dev.swiftstorm.akkaradb.common.binpack.TypeAdapter
import dev.swiftstorm.akkaradb.common.binpack.AdapterRegistry

// Minecraft Adventure Componentのアダプター例
private val componentAdapter = object : TypeAdapter<Component> {
    private val serializer = GsonComponentSerializer.gson()

    override fun estimateSize(value: Component): Int {
        val json = serializer.serialize(value)
        return 4 + json.length  // 4 bytes for size + JSON string
    }

    override fun write(value: Component, buffer: ByteBufferL) {
        val json = serializer.serialize(value)
        val bytes = json.toByteArray(Charsets.UTF_8)

        buffer.i32 = bytes.size
        buffer.putBytes(bytes)
    }

    override fun read(buffer: ByteBufferL): Component {
        val size = buffer.i32
        require(size >= 0) { "Negative size: $size" }
        require(buffer.remaining >= size) {
            "Insufficient bytes: need=$size remaining=${buffer.remaining}"
        }

        val bytes = ByteArray(size)
        repeat(size) { i ->
            bytes[i] = buffer.i8.toByte()
        }

        val json = String(bytes, Charsets.UTF_8)
        return serializer.deserialize(json)
    }
}

// 登録
AdapterRegistry.register<Component>(componentAdapter)

// 使用
data class ChatMessage(
    @Id val id: String,
    val sender: String,
    val message: Component  // カスタムアダプターで自動シリアライズ
)

val db = AkkDSL.open<ChatMessage, String>(base, StartupMode.NORMAL)
db.put(ChatMessage("msg1", "player1", Component.text("Hello!")))
```

### 重要な注意事項

#### 1. estimateSize の実装

```kotlin
// ✓ 正しい: 上限値を返す
override fun estimateSize(value: Component): Int {
    val json = serializer.serialize(value)
    return 4 + json.length  // 実際のサイズ
}

// ✓ 正しい: 余裕を持った上限値
override fun estimateSize(value: SomeType): Int {
    return 1024  // 最大1KBと仮定
}

// ✗ 間違い: 過小評価
override fun estimateSize(value: SomeType): Int {
    return 32  // 実際には100バイト必要 → BufferOverflowException
}
```

**ポイント:** `estimateSize`が過小評価しても自動リトライされますが、パフォーマンスが低下します。正確な上限値を返すのがベストです。

#### 2. buffer.position の管理

```kotlin
// ✓ 正しい: 相対アクセスでpositionが自動的に進む
override fun write(value: String, buffer: ByteBufferL) {
    buffer.i32 = value.length  // position += 4
    buffer.putBytes(value.toByteArray())  // position += bytes.size
}

// ✗ 間違い: positionを進めない
override fun write(value: String, buffer: ByteBufferL) {
    buffer.at(0).i32 = value.length  // 絶対アクセス（positionが進まない）
}
```

**ポイント:** `write`と`read`は必ず`buffer.position`を進める必要があります。相対アクセス（`buffer.i32`, `buffer.putBytes`等）を使用してください。

#### 3. 登録タイミング

```kotlin
// ✓ 正しい: データベースを開く前に登録
AdapterRegistry.register<Component>(componentAdapter)
val db = AkkDSL.open<ChatMessage, String>(base, StartupMode.NORMAL)

// ✗ 間違い: データベースを開いた後に登録
val db = AkkDSL.open<ChatMessage, String>(base, StartupMode.NORMAL)
AdapterRegistry.register<Component>(componentAdapter)  // 遅すぎる
```

**ポイント:** アダプターは最初のシリアライズ/デシリアライズより前に登録する必要があります。

#### 4. スレッドセーフティ

```kotlin
// ✓ 正しい: immutableまたはスレッドセーフ
private val componentAdapter = object : TypeAdapter<Component> {
    private val serializer = GsonComponentSerializer.gson()  // immutable
    // ...
}

// ✗ 間違い: 可変状態を持つ
private val badAdapter = object : TypeAdapter<SomeType> {
    private var counter = 0  // スレッドセーフでない
    override fun write(value: SomeType, buffer: ByteBufferL) {
        counter++  // 危険
    }
}
```

**ポイント:** `TypeAdapter`の実装はスレッドセーフである必要があります。

### アダプター管理API

```kotlin
// 登録確認
val hasAdapter = AdapterRegistry.hasCustomAdapter(Component::class)

// 登録解除
AdapterRegistry.unregisterAdapter(Component::class)

// 全カスタムアダプターをクリア
AdapterRegistry.clearAll()

// 統計情報取得
val stats = AdapterRegistry.getStats()
println("Custom adapters: ${stats.customAdapterCount}")
```

### デフォルトでサポートされる型

以下の型は標準でサポートされており、カスタムアダプターは不要です：

**プリミティブ型:**

- `Int`, `Long`, `Short`, `Byte`, `Boolean`, `Float`, `Double`, `Char`

**標準型:**

- `String`, `ByteArray`
- `UUID`, `BigInteger`, `BigDecimal`
- `LocalDate`, `LocalTime`, `LocalDateTime`, `Date`

**コレクション:**

- `List<T>`, `Set<T>`, `Map<K, V>`

**その他:**

- `Enum`（任意のEnum型）
- `Nullable<T>`（任意のnullable型）
- Kotlinデータクラス（自動リフレクション）

```

## 起動モード

Typed APIでは、用途に応じて起動モードを選択できます。

### StartupMode.NORMAL（推奨）

バランス型の設定。ほとんどのユースケースに適しています。

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.NORMAL)
```

**設定:**

- `k = 4, m = 2`
- `walFastMode = true`
- `walGroupN = 64`
- `walGroupMicros = 1_000`
- `flushMaxBlocks = 64`
- `flushMaxMicros = 1_000`
- `parityCoder = RSParityCoder(2)`

**特性:**

- 書き込みレイテンシ: 中程度
- 耐久性: 高い（WALグループコミット）

---

### StartupMode.FAST

書き込み速度優先。耐久性は若干低下します。

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.FAST)
```

**設定:**

- `k = 4, m = 1`
- `walFastMode = true`
- `walGroupN = 256`
- `walGroupMicros = 12_000`
- `flushMaxBlocks = 256`
- `flushMaxMicros = 2_000`
- `parityCoder = RSParityCoder(1)`

**特性:**

- 書き込みレイテンシ: 低い
- 耐久性: 中程度（最大12ms遅延）

---

### StartupMode.DURABLE

耐久性優先。書き込み速度は低下します。

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.DURABLE)
```

**設定:**

- `k = 4, m = 2`
- `walFastMode = false`
- `walGroupN = 1`
- `walGroupMicros = 0`
- `flushMaxBlocks = 32`
- `flushMaxMicros = 500`
- `parityCoder = RSParityCoder(2)`
- `durableCas = true`

**特性:**

- 書き込みレイテンシ: 高い
- 耐久性: 最高（即座にfsync）

---

### StartupMode.ULTRA_FAST

テスト用。fsyncを最小化します。**本番環境では使用しないことを推奨します。**

```kotlin
val db = AkkDSL.open<User, ShortUUID>(base, StartupMode.ULTRA_FAST)
```

**設定:**

- `k = 4, m = 1`
- `walFastMode = true`
- `walGroupN = 512`
- `walGroupMicros = 50_000`
- `flushMaxBlocks = 512`
- `flushMaxMicros = 50_000`
- `parityCoder = RSParityCoder(1)`

**特性:**

- 書き込みレイテンシ: 非常に低い
- 耐久性: 低い（クラッシュ時にデータ損失の可能性）

---

## Options設定

Low-level APIでは、詳細なチューニングが可能です。

### AkkaraDB.Options

```kotlin
data class Options(
    val baseDir: Path,
    val k: Int = 4,
    val m: Int = 2,
    val flushPolicy: FlushPolicy = FlushPolicy(
        maxBlocks = 32,
        maxMicros = 500
    ),
    val walFastMode: Boolean = true,
    val stripeFastMode: Boolean = true,
    val walGroupN: Int = 64,
    val walGroupMicros: Long = 1_000,
    val parityCoder: ParityCoder? = null,
    val durableCas: Boolean = false,
    val useStripeForRead: Boolean = false,
    val bloomFPRate: Double = 0.01,
    val debug: Boolean = false
)
```

### パラメータ詳細

#### baseDir

データベースのルートディレクトリ。以下のファイル/ディレクトリが作成されます:

- `wal.akwal` - Write-Ahead Log
- `manifest.akmf` - Manifestファイル
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
- `m = 1`: Reed-Solomonパリティ（1レーン故障まで復旧可能）
- `m = 2`: Reed-Solomonパリティ（2レーン故障まで復旧可能）
- `m ≥ 3`: Reed-Solomonパリティ（mレーン故障まで復旧可能）

**推奨:**

- 通常: `m = 2`（Reed-Solomon）
- 高信頼性: `m = 3`以上（Reed-Solomon）
- テスト: `m = 0`（パリティなし）

#### walGroupN

WALのグループコミット数。この数のエントリが貯まるか、`walGroupMicros`が経過するとfsyncが発行されます。

**調整指針:**

- 小さい値（1-32）: 低レイテンシ、低スループット
- 中間値（64-256）: バランス型
- 大きい値（512-1024）: 高スループット、高レイテンシ

#### walGroupMicros

WALのグループコミット時間（マイクロ秒）。

**調整指針:**

- `0-1_000 µs`: 低レイテンシ
- `1_000-12_000 µs`: バランス型
- `12_000-50_000 µs`: 高スループット（最大遅延）

#### walFastMode

`true`の場合、`force(false)`（fdatasync相当）を使用。`false`の場合、`force(true)`（fsync相当）を使用。

**推奨:** ほとんどの場合`true`

#### stripeFastMode

`true`の場合、Stripeのfsyncを非同期化。

**推奨:** ほとんどの場合`true`

#### durableCas

`true`の場合、CAS操作時にもWALに書き込む。

**推奨:** `false`（デフォルト）。耐久性が必要な場合は`true`。

#### useStripeForRead

`true`の場合、MemTableとSSTで見つからない場合にStripeからフォールバック読み取りを行う。

**推奨:** `false`（パフォーマンス優先）。デバッグ時のみ`true`。

---

## データ型

### MemRecord

Low-level APIの`range()`で返されるレコード型。

```kotlin
data class MemRecord(
    val key: ByteBufferL,
    val value: ByteBufferL,
    val seq: Long,
    val flags: Byte,
    val keyHash: Int,
    val approxSizeBytes: Int
) {
    val tombstone: Boolean
        get() = (flags.toInt() and 0x01) != 0

    companion object {
        val EMPTY: ByteBufferL = ByteBufferL.allocate(0)
    }
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
fun at(offset: Int): ByteBufferL

// 読み取り（Little Endian）
val i8: Int          // u8として読み取り（0-255）
val i16: Int         // i16として読み取り
val i32: Int         // i32として読み取り
val i64: Long        // i64として読み取り

// 書き込み（Little Endian）
fun put(src: ByteBufferL)
fun putBytes(bytes: ByteArray)

// ユーティリティ
fun duplicate(): ByteBufferL
fun asReadOnlyDuplicate(): ByteBufferL
fun rawDuplicate(): ByteBuffer
val remaining: Int
```

**使用例:**

```kotlin
val buf = ByteBufferL.allocate(1024)
buf.at(0).i32 = 42
val value = buf.at(0).i32
```

---

## Query API

Kotlinコンパイラプラグインにより、型安全なクエリAPIが利用可能です。

### query - クエリの構築

```kotlin
fun query(
    @AkkQueryDsl block: T.() -> Boolean
): AkkQuery
```

**注意:** このメソッドはコンパイラプラグインによって`akkQuery()`に書き換えられます。プラグインが適用されていない場合はエラーになります。

### runQ - クエリの実行

```kotlin
fun runQ(query: AkkQuery): Sequence<T>
```

**例:**

```kotlin
val users = db.runQ(db.query { age > 18 && name.startsWith("太") })
for (user in users) {
    println(user)
}
```

### 便利メソッド

```kotlin
// リストで取得
fun runToList(@AkkQueryDsl block: T.() -> Boolean): List<T>

// 最初の要素を取得
fun firstOrNull(@AkkQueryDsl block: T.() -> Boolean): T?

// 存在確認
fun exists(@AkkQueryDsl block: T.() -> Boolean): Boolean

// カウント
fun count(@AkkQueryDsl block: T.() -> Boolean): Int
```

**例:**

```kotlin
// 年齢が18歳より大きいユーザーのリスト
val adults = db.runToList { age > 18 }

// 名前が"太郎"のユーザーを最初の1件取得
val taro = db.firstOrNull { name == "太郎" }

// 年齢が30以上のユーザーが存在するか確認
val hasOldUsers = db.exists { age >= 30 }

// 年齢が20代のユーザー数
val twenties = db.count { age in 20..29 }
```

### サポートされる演算子

- 比較演算子: `==`, `!=`, `>`, `>=`, `<`, `<=`
- 論理演算子: `&&`, `||`, `!`
- null チェック: `x == null`, `x != null`
- コレクション: `x in collection`, `x !in collection`

**内部動作:**

1. コンパイラプラグインがラムダ式を`AkkExpr`AST表現に変換
2. `runQ()`がnamespace範囲のキーを`db.range()`で取得
3. 各エンティティをデシリアライズして述語を評価
4. 条件に一致する要素のみを返す

---

次へ: [アーキテクチャ](./ARCHITECTURE.md) | [ベンチマーク](./BENCHMARKS.md)

[概要に戻る](./ABOUT.md)

---