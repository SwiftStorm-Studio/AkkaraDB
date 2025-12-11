# ビルド

AkkaraDBをソースからビルドする方法を説明します。

## 📚 目次

- [前提条件](#前提条件)
- [リポジトリのクローン](#リポジトリのクローン)
- [ビルド手順](#ビルド手順)
- [テスト実行](#テスト実行)
- [モジュール別ビルド](#モジュール別ビルド)
- [IDEセットアップ](#ideセットアップ)
- [トラブルシューティング](#トラブルシューティング)
- [開発ガイドライン](#開発ガイドライン)

---

## 前提条件

### 必須

- **JDK 17以上**
  ```bash
  java -version
  # openjdk version "17.0.1" 2021-10-19
  ```

- **Git**
  ```bash
  git --version
  # git version 2.34.1
  ```

### 推奨

- **Kotlin 2.1以上** （Gradleが自動的にダウンロード）
- **IntelliJ IDEA 2024.1以上** （IDE使用時）

---

## リポジトリのクローン

```bash
# HTTPSでクローン
git clone https://github.com/SwiftStorm-Studio/AkkaraDB.git
cd AkkaraDB

# またはSSHでクローン
git clone git@github.com:SwiftStorm-Studio/AkkaraDB.git
cd AkkaraDB
```

### ブランチ構成

```
main       : 安定版（リリース用）
develop    : 開発版（最新機能）
feature/*  : 機能ブランチ
hotfix/*   : 緊急修正ブランチ
```

開発に参加する場合は`develop`ブランチをチェックアウト：

```bash
git checkout develop
```

---

## ビルド手順

### 全モジュールのビルド

```bash
# Gradleラッパーを使用（推奨）
./gradlew build

# Windows
gradlew.bat build
```

**出力:**

```
BUILD SUCCESSFUL in 45s
127 actionable tasks: 127 executed
```

**成果物:**

- `akkara/akkaradb/build/libs/akkaradb-0.2.9.jar` - Fat JAR（メイン成果物）
- `akkara/akkaradb/build/libs/akkaradb-0.2.9-thin.jar` - Thin JAR
- `akkara/akkaradb/build/libs/akkaradb-0.2.9-sources.jar` - ソースJAR

---

### クリーンビルド

```bash
# 全ての生成ファイルを削除してからビルド
./gradlew clean build
```

---

### 依存関係なしビルド（オフライン）

```bash
# 事前に依存関係をダウンロード
./gradlew build --refresh-dependencies

# オフラインビルド
./gradlew build --offline
```

---

## テスト実行

### 全テスト実行

```bash
./gradlew test
```

**出力例:**

```
> Task :akkara-engine:test
AkkaraDBTest > testPutAndGet() PASSED
AkkaraDBTest > testDelete() PASSED
AkkaraDBTest > testCompareAndSwap() PASSED
MemTableTest > testConcurrentWrites() PASSED
...

BUILD SUCCESSFUL in 12s
```

---

### 特定モジュールのテスト

```bash
# engineモジュールのみ
./gradlew :akkara-engine:test

# commonモジュールのみ
./gradlew :akkara-common:test
```

---

### プロパティテスト実行

```bash
# 長時間実行テスト（1M+ cases）
./gradlew :akkara-test:test --tests "*PropertyTest*"
```

---

### テストレポート確認

```bash
# テスト実行後、レポートを開く
open akkara/engine/build/reports/tests/test/index.html

# Linux
xdg-open akkara/engine/build/reports/tests/test/index.html
```

---

## モジュール別ビルド

### akkara/common

基礎プリミティブモジュール：

```bash
./gradlew :akkara-common:build
```

**成果物:**

- `akkara/common/build/libs/akkara-common-0.2.9.jar`

---

### akkara/engine

ストレージエンジン本体：

```bash
./gradlew :akkara-engine:build
```

**依存:**

- `akkara-common`
- `akkara-format-api`
- `akkara-format-akk`

---

### akkara/plugin

Kotlinコンパイラプラグイン：

```bash
# Gradleプラグイン
./gradlew :akkara-plugin:build

# コンパイラプラグイン
./gradlew :akkara-compiler:build
```

**成果物:**

- `akkara/plugin/akkara-plugin/build/libs/akkara-plugin-0.1.0.jar`
- `akkara/plugin/akkara-compiler/build/libs/akkara-compiler-0.3.9.jar`

---

### akkara/akkaradb（統合モジュール）

Fat JARの生成：

```bash
./gradlew :akkaradb:shadowJar
```

**成果物:**

- `akkara/akkaradb/build/libs/akkaradb-0.2.9.jar` - 全依存関係を含む

**除外される依存:**

- Kotlin標準ライブラリ（実行環境に存在する前提）
- Kotlinxライブラリ
- JetBrains annotations

---

## IDEセットアップ

### IntelliJ IDEA

#### プロジェクトのインポート

1. IntelliJ IDEAを起動
2. `File` → `Open...`
3. `AkkaraDB`ディレクトリを選択
4. `Trust Project`をクリック

IntelliJがGradleプロジェクトを自動認識し、依存関係をダウンロードします。

---

#### Kotlinプラグインの設定

1. `File` → `Settings` → `Plugins`
2. `Kotlin`を検索してインストール（通常はプリインストール済み）
3. バージョン確認: `2.1.0`以上

---

#### コンパイラ設定

`File` → `Settings` → `Build, Execution, Deployment` → `Compiler` → `Kotlin Compiler`

```
Language version: 2.1
API version: 2.1
JVM target: 17
```

---

#### Gradle JVM設定

`File` → `Settings` → `Build, Execution, Deployment` → `Build Tools` → `Gradle`

```
Gradle JVM: 17 (推奨: Amazon Corretto 17 または OpenJDK 17)
Build and run using: Gradle
Run tests using: Gradle
```

---

#### 推奨プラグイン

- **Kotlin** (必須)
- **Gradle** (必須)
- **GitToolBox** (便利)
- **Rainbow Brackets** (便利)

---

### Visual Studio Code

#### 拡張機能のインストール

```json
// .vscode/extensions.json
{
  "recommendations": [
    "mathiasfrohlich.kotlin",
    "vscjava.vscode-java-pack",
    "vscjava.vscode-gradle"
  ]
}
```

インストール：

```bash
code --install-extension mathiasfrohlich.kotlin
code --install-extension vscjava.vscode-java-pack
code --install-extension vscjava.vscode-gradle
```

---

#### タスク設定

```json
// .vscode/tasks.json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "Build AkkaraDB",
      "type": "shell",
      "command": "./gradlew build",
      "group": {
        "kind": "build",
        "isDefault": true
      }
    },
    {
      "label": "Test AkkaraDB",
      "type": "shell",
      "command": "./gradlew test",
      "group": "test"
    }
  ]
}
```

---

## トラブルシューティング

### ビルドエラー: "Cannot find symbol ByteBufferL"

**原因:** モジュール依存関係が解決されていない

**解決策:**

```bash
./gradlew clean build --refresh-dependencies
```

---

### ビルドエラー: "Kotlin compiler version mismatch"

**原因:** IDEのKotlinバージョンとプロジェクトのバージョンが不一致

**解決策:**

```bash
# build.gradle.ktsでバージョン確認
cat build.gradle.kts | grep "kotlin"

# IntelliJのKotlinプラグインを更新
# File → Settings → Plugins → Kotlin → Update
```

---

### テスト失敗: "WAL replay failed"

**原因:** 前回のテスト実行時のデータが残っている

**解決策:**

```bash
# テストデータディレクトリをクリーン
rm -rf akkara/engine/build/test-data
./gradlew clean test
```

---

### Out of Memory エラー

**原因:** Gradleデーモンのメモリ不足

**解決策:**

```bash
# gradle.properties に追加
echo "org.gradle.jvmargs=-Xmx4g -XX:MaxMetaspaceSize=512m" >> gradle.properties

# Gradleデーモンを再起動
./gradlew --stop
./gradlew build
```

---

### コンパイラプラグインエラー: "IR lowering failed"

**原因:** `kotlin.compiler.execution.strategy`が設定されていない

**解決策:**

```bash
# gradle.properties に追加
echo "kotlin.compiler.execution.strategy=in-process" >> gradle.properties
./gradlew clean build
```

詳細は[インストール](./INSTALLATION.md#コンパイラプラグイン設定必須)を参照してください。

---

## 開発ガイドライン

### コーディングスタイル

AkkaraDBはKotlinの標準コーディング規約に従います：

```kotlin
// クラス名: PascalCase
class MemTable

// 関数名: camelCase
fun put(key: ByteBufferL, value: ByteBufferL)

// 定数: UPPER_SNAKE_CASE
const val BLOCK_SIZE = 32 * 1024

// プライベート変数: camelCase with underscore prefix
private val _sealed = AtomicBoolean(false)
```

---

### コミットメッセージ規約

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Type:**

- `feat`: 新機能
- `fix`: バグ修正
- `docs`: ドキュメント
- `style`: フォーマット（コード動作に影響なし）
- `refactor`: リファクタリング
- `perf`: パフォーマンス改善
- `test`: テスト追加/修正
- `chore`: ビルドプロセス/補助ツール

**例:**

```
feat(engine): add compareAndSwap support

Implement CAS operation for MemTable with WAL durability.
Includes retry logic for concurrent updates.

Closes #123
```

---

### ブランチ戦略

```
main
  └─ develop
       ├─ feature/cas-support
       ├─ feature/bloom-filter
       └─ hotfix/wal-corruption
```

**ワークフロー:**

```bash
# 新機能開発
git checkout develop
git checkout -b feature/my-feature
# ... 開発 ...
git commit -m "feat(engine): add my feature"
git push origin feature/my-feature
# → Pull Request to develop

# 緊急修正
git checkout main
git checkout -b hotfix/critical-bug
# ... 修正 ...
git commit -m "fix(wal): resolve corruption issue"
git push origin hotfix/critical-bug
# → Pull Request to main & develop
```

---

### プルリクエスト

**チェックリスト:**

- [ ] コードが規約に従っている
- [ ] テストが追加されている
- [ ] 全テストがパスしている
- [ ] ドキュメントが更新されている（必要な場合）
- [ ] コミットメッセージが規約に従っている

**テンプレート:**

```markdown
## 概要

<!-- 変更内容の簡潔な説明 -->

## 変更理由

<!-- なぜこの変更が必要か -->

## 変更内容

<!-- 技術的な詳細 -->

## テスト

<!-- どのようにテストしたか -->

## 関連Issue

Closes #XXX
```

---

### ローカルでのパブリッシュテスト

```bash
# Maven Localにパブリッシュ
./gradlew publishToMavenLocal

# 成果物確認
ls ~/.m2/repository/dev/swiftstorm/akkaradb/0.2.9/

# 別プロジェクトから参照テスト
dependencies {
    implementation("dev.swiftstorm:akkaradb:0.2.9")
}
```

---

### パフォーマンステスト実行

```bash
# ベンチマークモジュール実行
./gradlew :akkara-test:jmh

# 特定のベンチマークのみ
./gradlew :akkara-test:jmh -Pinclude=".*WriteBenchmark.*"

# 結果確認
cat akkara/test/build/reports/jmh/results.txt
```

---

### デバッグビルド

```bash
# デバッグ情報付きでビルド
./gradlew build -Pdebug=true

# ログレベルを上げる
./gradlew build --debug

# スタックトレース表示
./gradlew build --stacktrace
```

---

## 継続的インテグレーション

AkkaraDBはGitHub Actionsを使用しています。

### ワークフロー

```yaml
# .github/workflows/build.yml
name: Build
on: [ push, pull_request ]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          distribution: 'corretto'
          java-version: '17'
      - run: ./gradlew build test
```

---

### ローカルでのCI再現

```bash
# Actを使用（GitHub Actions互換）
brew install act  # macOS
# または
sudo apt install act  # Linux

# ワークフロー実行
act push
```

---

## リリースプロセス

### バージョニング

セマンティックバージョニング（SemVer）を使用：

```
MAJOR.MINOR.PATCH

例:
0.2.9  - パッチリリース（バグ修正）
0.3.0  - マイナーリリース（新機能、後方互換性あり）
1.0.0  - メジャーリリース（破壊的変更）
```

---

### リリース手順

```bash
# 1. バージョン更新
# build.gradle.kts の version を更新

# 2. CHANGELOG更新
echo "## [0.2.10] - 2025-01-15
### Added
- New feature X
### Fixed
- Bug Y" >> CHANGELOG.md

# 3. コミット＆タグ
git add build.gradle.kts CHANGELOG.md
git commit -m "chore: release v0.2.10"
git tag v0.2.10

# 4. プッシュ
git push origin develop
git push origin v0.2.10

# 5. パブリッシュ
./gradlew publishAllModule
```

---

## 貢献方法

AkkaraDBへの貢献を歓迎します！

### バグレポート

[GitHub Issues](https://github.com/SwiftStorm-Studio/AkkaraDB/issues)で報告してください：

```markdown
**環境:**

- OS: Ubuntu 22.04
- JDK: OpenJDK 17
- AkkaraDB: v0.2.9

**再現手順:**

1. ...
2. ...

**期待される動作:**
...

**実際の動作:**
...

**ログ:**
...
```

---

### 機能リクエスト

[GitHub Discussions](https://github.com/SwiftStorm-Studio/AkkaraDB/discussions)で提案してください：

```markdown
**機能の説明:**
...

**ユースケース:**
...

**代替案:**
...
```

---

### プルリクエスト

1. Issueを作成（既存のIssueがない場合）
2. `main`ブランチからフォーク
3. `feature/your-feature`ブランチを作成
4. 変更を実装
5. テストを追加
6. プルリクエストを作成

---

## リソース

- **GitHub リポジトリ**: https://github.com/SwiftStorm-Studio/AkkaraDB
- **Maven リポジトリ**: https://repo.swiftstorm.dev/maven2/
- **ドキュメント**: [目次](../README.md)
- **Discord コミュニティ**: https://discord.swiftstorm.dev

---

次へ: [概要](./ABOUT.md) | [API リファレンス](./API_REFERENCE.md)

[目次に戻る](../README.md)

---