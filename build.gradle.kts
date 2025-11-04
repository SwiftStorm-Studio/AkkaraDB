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
        "akkara-cli" -> "0.0.1-SNAPSHOT"
        "akkara-common" -> "0.0.1+alpha.1"
        "akkara-engine" -> "0.0.1+alpha.1"
        "akkara-format-api" -> "0.0.1+alpha.1"
        "akkara-format-akk" -> "0.0.1+alpha.1"
//        "akkara-java-api" -> "0.0.1-SNAPSHOT"
//        "akkara-replica" -> "0.0.1-SNAPSHOT"
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
                    implementation(kotlin("stdlib"))
                    implementation(kotlin("reflect"))
                    implementation("org.objenesis:objenesis:3.4")
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
                implementation(platform("io.netty:netty-bom:4.2.2.Final"))

                implementation("io.netty:netty-transport-classes-io_uring")
            }
        }

        "akkara-engine" -> {
            dependencies {
                implementation(project(":akkara-common"))
                implementation(project(":akkara-format-api"))
                implementation(project(":akkara-format-akk"))

                implementation("org.objenesis:objenesis:3.4")

                implementation(kotlin("reflect"))
                implementation(kotlin("serialization"))
                implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.9.0")
                implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.9.0")
                implementation("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.9.0")
            }
        }

//        "akkara-replica" -> {
//            dependencies {
//                implementation(project(":akkara-common"))
//                implementation(project(":akkara-engine"))
//            }
//        }

//        "akkara-java-api" -> {
//            dependencies {
//                implementation(project(":akkara-engine"))
//            }
//        }

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
        withJavadocJar()

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
                "https://repo.ririfa.net/maven2-snap/"
            } else {
                "https://repo.ririfa.net/maven2-rel/"
            }

            val artifactUrl = "${repoUrl}net/ririfa/$artifactId/$ver/$artifactId-$ver.jar"
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

    tasks.register<Jar>("plainJar") {
        group = "ririfa"
        description = "Project classes only"
        dependsOn("classes")
        archiveClassifier.set("")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        from(sourceSets.main.get().output)
    }

//    tasks.register<ShadowJar>("relocatedFatJar") {
//        dependsOn("classes")
//        group = "ririfa"
//        description = "Creates a relocated fat jar containing shadedAPI dependencies"
//        archiveClassifier.set("fat")
//        from(sourceSets.main.get().output)
//    }

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

                artifact(tasks.named<Jar>("plainJar"))
//                artifact(tasks.named<ShadowJar>("relocatedFatJar"))
                artifact(tasks.named<Jar>("sourcesJar"))
                artifact(tasks.named<Jar>("dokkaHtmlJar"))

                pom {
                    name.set(project.name)
                    description.set("")
                    url.set("https://github.com/ririf4/Yacla")
                    licenses {
                        license {
                            name.set("MIT")
                            url.set("https://opensource.org/license/mit")
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
                val releasesRepoUrl = uri("https://repo.ririfa.net/maven2-rel/")
                val snapshotsRepoUrl = uri("https://repo.ririfa.net/maven2-snap/")
                url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

                credentials {
                    username = findProperty("nxUN").toString()
                    password = findProperty("nxPW").toString()
                }
            }
        }
    }
}