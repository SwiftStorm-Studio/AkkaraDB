# Installation

Instructions for setting up AkkaraDB in your project.

## 📊 Version Check

Latest versions can be verified via these badges:

- **AkkaraDB
  **: ![AkkaraDB Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkaradb/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)
- **AkkaraPlugin
  **: ![AkkaraDB Plugin Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkara-plugin/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)
- **AkkaraCompiler
  **: ![AkkaraDB Compiler Version](https://img.shields.io/badge/dynamic/xml?url=https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkara-compiler/maven-metadata.xml&query=/metadata/versioning/latest&style=plastic&logo=sonatype&label=Nexus)

## 📋 Requirements

- **JDK 17 or higher**
- **Kotlin 2.2.21 or higher** (when using Typed API)
- **Gradle** or **Maven**

## 📦 Repository Configuration

AkkaraDB is distributed through a custom Maven repository.

### Gradle (Kotlin DSL)

Add the following to **settings.gradle.kts**:

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

Add the following to **build.gradle.kts**:

```kotlin
plugins {
    kotlin("jvm") version "2.2.21" // Optional
    id("dev.swiftstorm.akkaradb-plugin") version "0.1.0" // For Typed API
}

dependencies {
    akkara("0.2.9", "implementation") // Specify version and scope
}
```

### Gradle (Groovy DSL)

Add the following to **settings.gradle**:

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

Add the following to **build.gradle**:

```groovy
plugins {
    id 'org.jetbrains.kotlin.jvm' version '2.2.21' // Optional
    id 'dev.swiftstorm.akkaradb-plugin' version '0.1.0' // For Typed API
}

dependencies {
    implementation 'dev.swiftstorm:akkaradb:0.2.9' // Specify version
}
```

### Maven

Add the following to **pom.xml**:

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

## ⚙️ Compiler Plugin Configuration (Required)

Compiler plugin configuration is **required** to use Typed API (AkkDSL) and Query DSL.

### Gradle

Add the following to **gradle.properties**:

```properties
kotlin.compiler.execution.strategy=in-process
```

### Maven

Choose one of the following methods:

#### Method 1: Recommended (Create `.mvn/maven.config`)

Create a `.mvn/maven.config` file at project root:

```
-Dkotlin.compiler.execution.strategy=in-process
```

#### Method 2: Command Line

```bash
mvn clean compile -Dkotlin.compiler.execution.strategy=in-process
```

### ⚠️ Why Is This Configuration Necessary?

The Akkara compiler plugin transforms Kotlin's IR (intermediate representation) to optimize Query DSL (operators like `&&`, `||`) into optimized query
expressions. When Kotlin compilation runs in `daemon` mode (default), the plugin execution order becomes unstable, causing the transformation to occur after JVM
optimization phases, which can result in compilation errors.

Setting `in-process` mode ensures the plugin runs in the same process as the build tool, guaranteeing correct execution order.

## 🎯 Minimal Configuration (Low-level API Only)

If you use only the low-level API without Typed API, the compiler plugin is not required.

**build.gradle.kts**:

```kotlin
dependencies {
    implementation("dev.swiftstorm:akkaradb:0.2.9")
}
```

In this case, only direct manipulation via `ByteBufferL` is available.

---

Next Steps: [Quick Start](./QUICKSTART.md) | [API Reference](./API_REFERENCE.md)

---