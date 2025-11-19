import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.text.SimpleDateFormat
import java.util.*

plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.dokka)
    alias(libs.plugins.kotlin.serialization)
    `maven-publish`
}

allprojects {
    apply(plugin = "kotlin")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "maven-publish")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")

    group = "dev.swiftstorm"
    version = when (name) {
        "akkaradb" -> "0.0.1+rc.4"
        "akkara-plugin" -> "0.0.1+rc.1"
        else -> "0.0.0+dev-${SimpleDateFormat("yyyyMMdd-HHmmss").format(Date())}"
    }
    description = ""

    repositories {
        mavenCentral()
        maven("https://repo.swiftstorm.dev/maven2/") { name = "SwiftStorm Repository" }
    }
}

subprojects {
    when (name) {
        "akkara-common" -> {
            afterEvaluate {
                dependencies {
                    compileOnly(libs.slf4j)
                    compileOnly(kotlin("reflect"))
                }
            }
        }

        "akkara-format-api" -> {
            dependencies {
                implementation(project(":akkara-common"))
            }
        }

        "akkara-format-akk" -> {
            dependencies {
                implementation(project(":akkara-common"))
                implementation(project(":akkara-format-api"))
            }
        }

        "akkara-engine" -> {
            dependencies {
                implementation(project(":akkara-common"))
                implementation(project(":akkara-format-api"))
                implementation(project(":akkara-format-akk"))

                compileOnly(kotlin("reflect"))
                compileOnly(kotlin("serialization"))
            }
        }

        "akkara-plugin" -> {
            afterEvaluate {
                dependencies {
                    compileOnly("org.jetbrains.kotlin:kotlin-compiler-embeddable:${libs.versions.kotlin.get()}")
                    compileOnly("org.jetbrains.kotlin:kotlin-gradle-plugin-api:${libs.versions.kotlin.get()}")
                }
            }
        }

        "akkara-test" -> {
            dependencies {
                testImplementation(kotlin("test"))
                testImplementation(project(":akkara-common"))
                testImplementation(project(":akkara-format-api"))
                testImplementation(project(":akkara-format-akk"))
                testImplementation(project(":akkara-engine"))
                testImplementation("org.junit.jupiter:junit-jupiter-api:5.13.1")
                testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.13.1")
            }
        }
    }

    java {
        withSourcesJar()

        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlin {
        jvmToolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    tasks.withType<KotlinCompile> {
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
        }
    }

    tasks.withType<JavaCompile> {
        options.release.set(17)
    }

    tasks.named<Jar>("jar") {
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        archiveClassifier.set("")
    }

    tasks.named<Jar>("sourcesJar") {
        from(sourceSets["main"].allSource)
    }
}