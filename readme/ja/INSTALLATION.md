# インストール

AkkaraDBをプロジェクトにセットアップする方法を説明します。

## 📊 バージョン確認

最新バージョンは以下のバッジで確認できます:

- **AkkaraDB
  **: ![AkkaraDB Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkaradb/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)
- **AkkaraPlugin
  **: ![AkkaraDB Plugin Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkara-plugin/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)
- **AkkaraCompiler
  **: ![AkkaraDB Compiler Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkara-compiler/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)

## 📋 要件

- **JDK 17以上**
- **Kotlin 2.1以上** （Typed API使用時）
- **Gradle** または **Maven**

## 📦 リポジトリ設定

AkkaraDBは独自のMavenリポジトリで配布されています。

### Gradle (Kotlin DSL)

**settings.gradle.kts**に以下を追加:

```kotlin
pluginManagement {
    repositories {
        gradlePluginPortal()
        maven("https://repo.swiftstorm.dev/maven2/")
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven("https://repo.swiftstorm.dev/maven2/")
    }
}
```

**build.gradle.kts**に以下を追加:

```kotlin
plugins {
    kotlin("jvm") version "2.2.21" // 任意
    id("dev.swiftstorm.akkaradb-plugin") version "0.1.0" // Typed API用
}

dependencies {
    akkara("0.2.9", "implementation") // バージョンとスコープを指定して追加できます
}
```

### Gradle (Groovy DSL)

**settings.gradle**に以下を追加:

```groovy
pluginManagement {
    repositories {
        gradlePluginPortal()
        maven { url 'https://repo.swiftstorm.dev/maven2/' }
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven { url 'https://repo.swiftstorm.dev/maven2/' }
    }
}
```

**build.gradle**に以下を追加:

```groovy
plugins {
    id 'org.jetbrains.kotlin.jvm' version '2.2.21' // 任意
    id 'dev.swiftstorm.akkaradb-plugin' version '0.1.0' // Typed API用
}

dependencies {
    akkara('0.2.9')
    // or akkara('0.2.9', 'implementation')
}
```

### Maven

**pom.xml**に以下を追加:

```xml

<repositories>
    <repository>
        <id>swiftstorm-maven</id>
        <url>https://repo.swiftstorm.dev/maven2/</url>
    </repository>
</repositories>

<dependencies>
<dependency>
    <groupId>dev.swiftstorm</groupId>
    <artifactId>akkaradb</artifactId>
    <version>0.2.9</version>
</dependency>
</dependencies>
```

## ⚙️ コンパイラプラグイン設定（必須）

Typed API（AkkDSL）とクエリDSLを使用するには、コンパイラプラグインの設定が**必須**です。

### Gradle

**gradle.properties**に以下を追加:

```properties
kotlin.compiler.execution.strategy=in-process
```

### Maven

以下のいずれかの方法を選択:

#### 方法1: 推奨（`.mvn/maven.config`を作成）

プロジェクトルートに`.mvn/maven.config`ファイルを作成し、以下を記述:

```
-Dkotlin.compiler.execution.strategy=in-process
```

#### 方法2: コマンドライン指定

```bash
mvn clean compile -Dkotlin.compiler.execution.strategy=in-process
```

### ⚠️ なぜこの設定が必要？

AkkaraコンパイラプラグインはKotlinのIR（中間表現）を変換して、クエリDSL（`&&`, `||`演算子など）を最適化されたクエリ式に変換します。Kotlinコンパイルが`daemon`
モード（デフォルト）で実行されると、プラグインの実行順序が不安定になり、変換がJVM最適化フェーズの後に行われてコンパイルエラーが発生する可能性があります。

`in-process`モードに設定することで、プラグインがビルドツールと同じプロセスで実行され、正しい実行順序が保証されます。

## 🎯 最小構成（低レベルAPIのみ）

Typed APIを使用せず、低レベルAPIのみを使う場合は、コンパイラプラグインは不要です。

**build.gradle.kts**:

```kotlin
dependencies {
    implementation("dev.swiftstorm:akkaradb:0.2.9")
}
```

この場合、`ByteBufferL`による直接操作のみが利用可能です。

---

次のステップ: [クイックスタート](./QUICKSTART.md) | [API リファレンス](./API_REFERENCE.md)

---