# AkkaraDB

[![AkkaraDB Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkaradb/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)](https://repo.ririfa.net/service/rest/repository/browse/maven-public/dev/swiftstorm/akkaradb/)

JVM上で動作する超低レイテンシな組み込みキーバリューストア

### Documents are under construction!! Please wait for a while!!

---

## 📚 ドキュメント

### 日本語 (Japanese)

- [📖 概要](./readme/ja/ABOUT.md) - AkkaraDBとは？主な特徴
- [⚡ クイックスタート](./readme/ja/QUICKSTART.md) - 5分で始める
- [📦 インストール](./readme/ja/INSTALLATION.md) - セットアップ手順
- [🏗️ アーキテクチャ](./readme/ja/ARCHITECTURE.md) - 内部設計の詳細
- [📘 API リファレンス](./readme/ja/API_REFERENCE.md) - API仕様
- [⚡ ベンチマーク](./readme/ja/BENCHMARKS.md) - パフォーマンス測定結果
- [🔧 ビルド](./readme/ja/BUILD.md) - ソースからビルド

### English

- [📖 About](./readme/en/ABOUT.md) - What is AkkaraDB?
- [⚡ Quick Start](./readme/en/QUICKSTART.md) - Get started in 5 minutes
- [📦 Installation](./readme/en/INSTALLATION.md) - Setup guide
- [🏗️ Architecture](./readme/en/ARCHITECTURE.md) - Internal design
- [📘 API Reference](./readme/en/API_REFERENCE.md) - API specification
- [⚡ Benchmarks](./readme/en/BENCHMARKS.md) - Performance results
- [🔧 Build](./readme/en/BUILD.md) - Build from source

---

## 🚀 クイックスタート

```kotlin
// データモデル定義
data class User(val name: String, val age: Int)

val base = java.nio.file.Paths.get("./data/akkdb")
val users = dev.swiftstorm.akkaradb.engine.AkkDSL.open<User>(base, dev.swiftstorm.akkaradb.engine.StartupMode.NORMAL)

// 書き込み・読み取り
val id = dev.swiftstorm.akkaradb.common.ShortUUID.generate()
users.put("user", id, User(name = "太郎", age = 42))
val user = users.get("user", id)

users.close()
```

詳細は[クイックスタート](./readme/ja/QUICKSTART.md)を参照してください。

---

## 📊 パフォーマンス概要

| 指標           | 目標          | 達成値                                          |
|:-------------|:------------|:---------------------------------------------|
| 書き込みP99レイテンシ | ≤ 200 µs    | **≤ 60 µs**                                  |
| 読み取りP99レイテンシ | ≤ 20 µs     | **≈ 12 µs**                                  |
| 持続スループット     | ≥ 10k ops/s | **≈ 30k ops/s (書込)** / **≈ 360k ops/s (読取)** |

詳細は[ベンチマーク](./readme/ja/BENCHMARKS.md)を参照してください。

---

## 📄 ライセンス

GNU Lesser General Public License v3.0 (LGPL-3.0)

---

## 🔗 リンク

- [GitHub Repository](https://github.com/SwiftStorm-Studio/AkkaraDB)
- [Maven Repository](https://repo.swiftstorm.dev/maven2/)