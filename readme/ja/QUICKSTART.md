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

// 1. データモデル定義（@IdアノテーションでIDフィールドを指定）
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

    // 3. データを書き込む（IDを指定）
    users.put(
        "user001",
        User(
            id = "user001",
            name = "山田太郎",
            age = 28,
            email = "yamada@example.com"
        )
    )
    println("書き込み完了: user001")

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

**@Idアノテーションなしでエンティティ自体からIDを抽出する場合:**

```kotlin
data class User(
    @Id val id: String,
    val name: String,
    val age: Int
)

// エンティティからIDを自動抽出して書き込み
users.put(User(id = "user001", name = "太郎", age = 25))
```

### Low-level API

`ByteBufferL`を使った直接操作:

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

    // 3. 書き込み
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

## 🎛️ 起動モード

Typed APIでは、用途に応じて起動モードを選択できます:

```kotlin
// バランス型（推奨）
val db = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// 高速書き込み優先（耐久性は若干低下）
val db = AkkDSL.open<User, String>(base, StartupMode.FAST)

// 耐久性優先（書き込み速度は低下）
val db = AkkDSL.open<User, String>(base, StartupMode.DURABLE)

// 超高速（テスト用、fsync最小化）
val db = AkkDSL.open<User, String>(base, StartupMode.ULTRA_FAST)
```

各モードの詳細は[API リファレンス](./API_REFERENCE.md#起動モード)を参照してください。

## 🔍 範囲検索とクエリ

### クエリDSL

型安全なクエリでフィルタリング:

```kotlin
data class User(
    @Id val id: String,
    val name: String,
    val age: Int,
    val isActive: Boolean
)

val users = AkkDSL.open<User, String>(base, StartupMode.NORMAL)

// 年齢が25歳以上かつアクティブなユーザーを検索
val results = users.runToList { age >= 25 && isActive }

for (user in results) {
    println(user)
}

// 最初の1件のみ取得
val firstUser = users.firstOrNull { age >= 30 }

// 存在確認
val exists = users.exists { name == "太郎" }

// 件数カウント
val count = users.count { age < 20 }
```

## 🔄 upsert（更新または挿入）

データが存在しない場合は作成、存在する場合は更新:

```kotlin
data class Counter(
    @Id val id: String,
    var count: Int = 0
)

val counters = AkkDSL.open<Counter, String>(base, StartupMode.NORMAL)

// カウンターをインクリメント（存在しなければ作成）
counters.upsert("counter1") {
    count += 1
}
```

## 🛠️ オプション設定

詳細なチューニングが必要な場合は、低レベルAPIの`Options`を使用:

```kotlin
val db = AkkaraDB.open(
    AkkaraDB.Options(
        baseDir = Paths.get("./data/akkdb"),
        k = 4,                      // データレーン数
        m = 2,                      // パリティレーン数
        walGroupN = 512,            // WALグループコミット数
        walGroupMicros = 50_000,    // WALグループコミット時間(µs)
        stripeFastMode = true,      // Stripe高速モード
        walFastMode = true          // WAL高速モード
    )
)
```

パラメータの詳細は[API リファレンス](./API_REFERENCE.md#options設定)を参照してください。

---

次へ: [API リファレンス](./API_REFERENCE.md) | [アーキテクチャ](./ARCHITECTURE.md) | [ベンチマーク](./BENCHMARKS.md)

[概要に戻る](./ABOUT.md)

---