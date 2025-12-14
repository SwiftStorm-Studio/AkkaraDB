import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.text.SimpleDateFormat
import java.util.*

plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.dokka)
    alias(libs.plugins.kapt)
    `maven-publish`
    application
}

tasks.register("publishAllModule") {
    dependsOn(
        ":akkaradb:publish",
        ":akkara-plugin:publish",
        ":akkara-compiler:publish"
    )
}

tasks {
    build {
        dependsOn("clean")
        dependsOn(":akkaradb:shadowJar")
    }
}

allprojects {
    apply(plugin = "kotlin")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "maven-publish")

    group = "dev.swiftstorm"
    version = when (name) {
        "akkaradb" -> "0.3.1"
        "akkara-plugin" -> "0.1.0"
        "akkara-compiler" -> "0.3.9"
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
            afterEvaluate {
                dependencies {
                    implementation(project(":akkara-common"))
                    implementation(project(":akkara-format-api"))
                    implementation(project(":akkara-format-akk"))

                    compileOnly(kotlin("reflect"))
                    compileOnly(kotlin("serialization"))
                }
            }
        }

        "akkara-plugin" -> {
            dependencies {
                compileOnly(kotlin("gradle-plugin-api"))
            }
        }

        "akkara-compiler" -> {
            dependencies {
                compileOnly(kotlin("compiler"))
            }
        }

        "akkara-test" -> {
            apply(plugin = "application")

            dependencies {
                testImplementation(kotlin("test"))
                testImplementation(kotlin("reflect"))
                testImplementation(project(":akkara-common"))
                testImplementation(project(":akkara-format-api"))
                testImplementation(project(":akkara-format-akk"))
                testImplementation(project(":akkara-engine"))
                testImplementation("org.junit.jupiter:junit-jupiter-api:5.13.1")
                testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.13.1")
            }

            tasks.named<JavaExec>("run") {
                jvmArgs = listOf(
                    "-Xmx4G",
                    "-XX:+UseG1GC",
                    "-XX:+AlwaysPreTouch",
                    "-XX:MaxGCPauseMillis=50",
                    "-Dfile.encoding=UTF-8"
                )
            }
        }
    }

    java { withSourcesJar() }

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