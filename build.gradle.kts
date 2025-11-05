import org.jetbrains.dokka.gradle.tasks.DokkaGeneratePublicationTask
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.net.HttpURLConnection
import java.net.URI
import java.text.SimpleDateFormat
import java.util.*

plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.dokka)
    alias(libs.plugins.shadow)
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
        "akkara-plugin" -> "0.1.0-SNAPSHOT"
        "akkara-cli" -> "0.0.1-SNAPSHOT"
        "akkara-common" -> "0.0.1+alpha.1"
        "akkara-engine" -> "0.0.1+alpha.1"
        "akkara-format-api" -> "0.0.1+alpha.1"
        "akkara-format-akk" -> "0.0.1+alpha.1"
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
                    implementation(kotlin("reflect"))
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

                implementation(kotlin("reflect"))
                implementation(kotlin("serialization"))
            }
        }

        "akkara-plugin" -> {
            dependencies {
                compileOnly("org.jetbrains.kotlin:kotlin-compiler-embeddable:${libs.versions.kotlin.get()}")
                compileOnly("org.jetbrains.kotlin:kotlin-gradle-plugin-api:${libs.versions.kotlin.get()}")
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

    tasks.withType<PublishToMavenRepository>().configureEach {
        onlyIf {
            val artifactId = project.name
            val ver = project.version.toString()
            val repoUrl = if (ver.endsWith("SNAPSHOT")) {
                "https://repo.swiftstorm.dev/maven2-snap/"
            } else {
                "https://repo.swiftstorm.dev/maven2-rel/"
            }

            val artifactUrl = "${repoUrl}dev/swiftstorm/$artifactId/$ver/$artifactId-$ver.jar"
            logger.lifecycle("Checking existence of artifact at: $artifactUrl")

            val connection = URI(artifactUrl).toURL().openConnection() as HttpURLConnection
            connection.requestMethod = "HEAD"
            connection.connectTimeout = 3000
            connection.readTimeout = 3000

            val exists = connection.responseCode == HttpURLConnection.HTTP_OK
            connection.disconnect()

            if (exists) {
                logger.lifecycle("Artifact already exists at $artifactUrl, skipping publish.")
                false
            } else {
                logger.lifecycle("Artifact not found at $artifactUrl, proceeding with publish.")
                true
            }
        }
    }

    tasks.register<Jar>("dokkaHtmlJar") {
        group = "dokka"
        description = "Generates HTML documentation using Dokka"
        dependsOn(tasks.named("dokkaGeneratePublicationHtml"))
        from(tasks.named<DokkaGeneratePublicationTask>("dokkaGeneratePublicationHtml").flatMap { it.outputDirectory })
        archiveClassifier.set("javadoc")
    }

    publishing {
        publications {
            create<MavenPublication>("maven") {
                groupId = project.group.toString()
                artifactId = project.name
                version = project.version.toString()

                artifact(tasks.named<Jar>("jar"))
                artifact(tasks.named<Jar>("sourcesJar"))
                artifact(tasks.named<Jar>("dokkaHtmlJar"))

                pom {
                    name.set(project.name)
                    description.set("")
                    url.set("https://github.com/SwiftStorm-Studio/AkkaraDB")
                    licenses {
                        license {
                            name.set("LGPL 3.0")
                            url.set("https://www.gnu.org/licenses/lgpl-3.0.ja.html")
                        }
                    }
                    developers {
                        developer {
                            id.set("ririfa")
                            name.set("RiriFa")
                            email.set("main@ririfa.net")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com/SwiftStorm-Studio/AkkaraDB.git")
                        developerConnection.set("scm:git:ssh://github.com/SwiftStorm-Studio/AkkaraDB.git")
                        url.set("https://github.com/SwiftStorm-Studio/AkkaraDB")
                    }
                }
            }
        }
        repositories {
            maven {
                val releasesRepoUrl = uri("https://repo.swiftstorm.dev/maven2-rel/")
                val snapshotsRepoUrl = uri("https://repo.swiftstorm.dev/maven2-snap/")
                url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

                credentials {
                    username = findProperty("nxUN").toString()
                    password = findProperty("nxPW").toString()
                }
            }
        }
    }
}