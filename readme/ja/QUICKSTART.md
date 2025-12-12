# クイックスタート

5分でAkkaraDBを使い始めるためのガイドです。

## 📝 前提条件

- [インストール](./INSTALLATION.md)が完了していること
- JDK 17以上がインストールされていること

## 🚀 基本的な使い方

### Typed API（推奨）

Kotlinのデータクラスを使った型安全なAPI:

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkDSL
import dev.swiftstorm.akkaradb.engine.StartupMode
import dev.swiftstorm.akkaradb.engine.Id
import java.nio.file.Paths

// 1. データモデル定義（@Idアノテーションでプライマリキーを指定）
data class User(
    @Id val id: String,
    val name: String,
    val age: Int,
    val email: String
)

fun main() {
    // 2. データベースを開く（型パラメータにエンティティ型とID型を指定）
    val base = Paths.get("./data/akkdb")
    val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

    // 3. データを書き込む
    // 方法1: IDとエンティティを別々に指定
    users.put(
        "user001",
        User(
            id = "user001",
            name = "山田太郎",
            age = 28,
            email = "yamada@example.com"
        )
    )

    // 方法2: エンティティから@Idを自動抽出
    users.put(User(
        id = "user002",
        name = "佐藤花子",
        age = 25,
        email = "sato@example.com"
    ))

    println("書き込み完了")

    // 4. データを読み取る
    val user = users.get("user001")
    println("読み取り結果: $user")

    // 5. データを削除
    users.delete("user001")
    println("削除完了")

    // 6. データベースを閉じる
    users.close()
}
```

---

### Low-level API

`ByteBufferL`を使った直接操作（高度な用途向け）:

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.common.ByteBufferL
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

fun main() {
    // 1. データベースを開く
    val base = Paths.get("./data/akkdb")
    val db = AkkaraDB.open(
        AkkaraDB.Options(baseDir = base)
    )

    // 2. キーと値を準備
    val key = ByteBufferL.wrap(
        StandardCharsets.UTF_8.encode("hello")
    ).position(0)

    val value = ByteBufferL.wrap(
        StandardCharsets.UTF_8.encode("world")
    ).position(0)

    // 3. 書き込み（シーケンス番号を返す）
    val seq = db.put(key, value)
    println("書き込み完了 (seq=$seq)")

    // 4. 読み取り
    val result = db.get(key)
    if (result != null) {
        val str = StandardCharsets.UTF_8.decode(result.rawDuplicate()).toString()
        println("読み取り結果: $str")
    }

    // 5. 削除
    db.delete(key)
    println("削除完了")

    // 6. フラッシュして閉じる
    db.flush()
    db.close()
}
```

---

## 🎛️ 起動モード

Typed APIでは、用途に応じて起動モードを選択できます:

```kotlin
// バランス型（推奨）
// k=4, m=2, walGroupN=64, walGroupMicros=1000
val db = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// 高速書き込み優先（耐久性は若干低下）
// k=4, m=1, walGroupN=256, walGroupMicros=12000
val db = AkkDSL.open<User, String>(base, StartupMode.FAST)

// 耐久性優先（書き込み速度は低下）
// walGroupN=1, walGroupMicros=0, durableCas=true
val db = AkkDSL.open<User, String>(base, StartupMode.DURABLE)

// 超高速（テスト用、fsync最小化）
// walGroupN=512, walGroupMicros=50000
val db = AkkDSL.open<User, String>(base, StartupMode.ULTRA_FAST)
```

各モードの詳細は[API リファレンス](./API_REFERENCE.md#起動モード)を参照してください。

---

## 🔍 クエリDSL

型安全なクエリでフィルタリング（Kotlinコンパイラプラグインが必要）:

```kotlin
data class User(
    @Id val id: String,
    val name: String,
    val age: Int,
    val isActive: Boolean
)

val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// データを追加
users.put(User("u001", "太郎", 30, true))
users.put(User("u002", "花子", 25, true))
users.put(User("u003", "次郎", 18, false))

// 年齢が25歳以上かつアクティブなユーザーを検索
val results = users.runToList { age >= 25 && isActive }
for (user in results) {
    println(user) // User(id=u001, ...), User(id=u002, ...)
}

// 最初の1件のみ取得
val firstUser = users.firstOrNull { age >= 30 }
println(firstUser) // User(id=u001, name=太郎, age=30, isActive=true)

// 存在確認
val exists = users.exists { name == "太郎" }
println(exists) // true

// 件数カウント
val count = users.count { age < 20 }
println(count) // 1
```

**サポートされる演算子:**

- 比較: `==`, `!=`, `>`, `>=`, `<`, `<=`
- 論理: `&&`, `||`, `!`
- null チェック: `field == null`, `field != null`
- コレクション: `in`, `!in`

---

## 🔄 upsert（更新または挿入）

データが存在しない場合は作成、存在する場合は更新:

```kotlin
data class Counter(
    @Id val id: String,
    var count: Int = 0
)

val counters = AkkDSL.open<Counter, String>(base, StartupMode.NORMAL)

// カウンターをインクリメント（存在しなければ新規作成）
counters.upsert("counter1") {
    count += 1
}

// 再度実行すると既存レコードが更新される
counters.upsert("counter1") {
    count += 1
}

val counter = counters.get("counter1")
println("Count: ${counter?.count}") // 2
```

**重要:** `upsert`を使用するには、エンティティクラスに**引数なしコンストラクタ**が必要です。

---

## 🛠️ 詳細設定（DSLカスタマイズ）

より細かい制御が必要な場合は、DSLビルダーを使用:

```kotlin
val users = AkkDSL.open<User, String>(base) {
    k = 4                       // データレーン数
    m = 2                       // パリティレーン数
    walGroupN = 128             // WALグループコミット数
    walGroupMicros = 2000       // WALグループコミット時間(µs)
    stripeFastMode = true       // Stripe高速モード
    walFastMode = true          // WAL高速モード
    bloomFPRate = 0.01          // Bloomフィルター偽陽性率
    debug = false               // デバッグログ
}
```

---

## 🔧 低レベルAPI Options

完全な制御が必要な場合は、`AkkaraDB.Options`を使用:

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.format.api.FlushPolicy
import dev.swiftstorm.akkaradb.format.akk.parity.RSParityCoder

val db = AkkaraDB.open(
    AkkaraDB.Options(
        baseDir = Paths.get("./data/akkdb"),
        k = 4,                                      // データレーン数
        m = 2,                                      // パリティレーン数
        flushPolicy = FlushPolicy(
            maxBlocks = 32,
            maxMicros = 500
        ),
        walFastMode = true,                         // WAL高速モード
        stripeFastMode = true,                      // Stripe高速モード
        walGroupN = 64,                             // WALグループコミット数
        walGroupMicros = 1_000,                     // WALグループコミット時間(µs)
        parityCoder = RSParityCoder(2),             // Reed-Solomonパリティ
        durableCas = false,                         // CASの耐久性
        useStripeForRead = false,                   // 読み取りにStripeを使用
        bloomFPRate = 0.01,                         // Bloom偽陽性率
        debug = false                               // デバッグモード
    )
)
```

パラメータの詳細は[API リファレンス](./API_REFERENCE.md#options設定)を参照してください。

---

## 📌 ベストプラクティス

### 1. @Idアノテーションの使用

```kotlin
// ✓ 正しい: プライマリキーを明示
data class User(
    @Id val id: String,
    val name: String
)

// ✗ 間違い: @Idアノテーションがない
data class User(
    val id: String,  // これだけではダメ
    val name: String
)
```

### 2. 必ずcloseを呼ぶ

```kotlin
// ✓ 推奨: use構文で自動クローズ
val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)
users.use {
    it.put(User("u001", "太郎", 30))
    val user = it.get("u001")
}

// または明示的にclose
try {
    val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)
    // ... 処理 ...
} finally {
    users.close()
}
```

### 3. ByteBufferLのposition管理

Low-level APIを使用する場合、`position(0)`を忘れずに:

```kotlin
// ✓ 正しい
val key = ByteBufferL.wrap(bytes).position(0)
db.get(key)

// ✗ 間違い: positionが不定
val key = ByteBufferL.wrap(bytes)
db.get(key) // 動作が不安定
```

### 4. 適切な起動モードの選択

```kotlin
// 本番環境: NORMAL（バランス型）
val prod = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// テスト環境: ULTRA_FAST（高速だが耐久性低）
val test = AkkDSL.open<User, String>(base, StartupMode.ULTRA_FAST)

// ミッションクリティカル: DURABLE（最大耐久性）
val critical = AkkDSL.open<User, String>(base, StartupMode.DURABLE)
```

---

## 🎯 次のステップ

基本的な使い方を理解したら、以下のドキュメントで詳細を学びましょう：

- **[API リファレンス](./API_REFERENCE.md)** - 全APIの詳細仕様
- **[アーキテクチャ](./ARCHITECTURE.md)** - 内部設計の理解
- **[ベンチマーク](./BENCHMARKS.md)** - パフォーマンス特性

---

## 💡 よくある質問

### Q: @Idアノテーションは複数のフィールドに付けられる？

A: いいえ。**1つのエンティティにつき1つの@Id**のみが必要です。複数付けるとエラーになります。

### Q: クエリDSLを使うにはコンパイラプラグインが必須？

A: はい。クエリDSL（`runToList { }`, `firstOrNull { }`
等）を使用するには、Kotlinコンパイラプラグインが必要です。詳細は[インストール](./INSTALLATION.md#コンパイラプラグイン設定)を参照してください。

### Q: StringやIntをIDとして使える？

A: はい。`String`, `Int`, `Long`, `UUID`など、シリアライズ可能な任意の型をIDとして使用できます。

### Q: 複数のPackedTableを同じディレクトリに開ける？

A: いいえ。1つのデータベースディレクトリに対して**1つのAkkaraDBインスタンスのみ**を開くことができます。複数のエンティティタイプを扱う場合は、異なるディレクトリを使用してください。

---

次へ: [API リファレンス](./API_REFERENCE.md) | [アーキテクチャ](./ARCHITECTURE.md) | [ベンチマーク](./BENCHMARKS.md)

[概要に戻る](./ABOUT.md)

---