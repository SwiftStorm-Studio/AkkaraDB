import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.withType
import org.jetbrains.dokka.gradle.tasks.DokkaGeneratePublicationTask
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.net.HttpURLConnection
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date
import java.util.jar.JarFile
import kotlin.text.startsWith

plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.dokka)
    alias(libs.plugins.shadow)
    `maven-publish`
}

allprojects {
    apply(plugin = "kotlin")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "maven-publish")

    group = "dev.swiftstorm"
    version = when (name) {
        "akkara-common" -> "0.0.1-SNAPSHOT"
        "akkara-core" -> "0.0.1-SNAPSHOT"
        "akkara-format-api" -> "0.0.1-SNAPSHOT"
        "akkara-format-akk" -> "0.0.1-SNAPSHOT"
        "akkara-format-cbor" -> "0.0.1-SNAPSHOT"
        "akkara-java-api" -> "0.0.1-SNAPSHOT"
        "akkara-replica" -> "0.0.1-SNAPSHOT"
        "akkara-wal" -> "0.0.1-SNAPSHOT"
        else -> "0.0.0+dev-${SimpleDateFormat("yyyyMMdd-HHmmss").format(Date())}"
    }
    description = ""

    repositories {
        mavenCentral()
        maven("https://repo.swiftstorm.dev/maven2/") { name = "SwiftStorm Repository" }
    }
}

subprojects {
    val akkaraAPI: Configuration by configurations.creating {
        isTransitive = false
        isCanBeConsumed = false
        isCanBeResolved = true
    }

    when (name) {
        "akkara-core" -> {
            dependencies {
                implementation(project(":akkara-common"))
                implementation(project(":akkara-format-api"))
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
        "akkara-format-cbor" -> {
            dependencies {
                implementation(project(":akkara-common"))
                implementation(project(":akkara-format-api"))
                akkaraAPI("org.jetbrains.kotlinx:kotlinx-serialization-cbor:1.8.1")
            }
        }
        "akkara-java-api" -> {
            dependencies {
                implementation(project(":akkara-core"))
            }
        }
        "akkara-replica" -> {
            dependencies {
                implementation(project(":akkara-core"))
                implementation(project(":akkara-format-api"))
            }
        }
        "akkara-wal" -> {
            dependencies {
                implementation(project(":akkara-core"))
                implementation(project(":akkara-format-api"))
            }
        }
    }

    afterEvaluate {
        akkaraAPI.forEach { logger.lifecycle("Shaded API: $it") }

        val artifacts = try {
            akkaraAPI.resolvedConfiguration.resolvedArtifacts
        } catch (e: Exception) {
            logger.warn("Could not resolve shadedAPI in ${project.name}: ${e.message}")
            emptySet<ResolvedArtifact>()
        }

        artifacts.forEach { artifact ->
            val id = artifact.moduleVersion.id
            val classifierPart = artifact.classifier?.let { ":$it" } ?: ""
            val notation = "${id.group}:${id.name}:${id.version}$classifierPart"
            logger.lifecycle("Pack: ${artifact.moduleVersion.id.group}")
            logger.lifecycle("Name: ${artifact.moduleVersion.id.name}")
            logger.lifecycle("Automatically adding to api: $notation in ${project.name}")
            dependencies.add("api", notation)
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

    tasks.register<ShadowJar>("relocatedFatJar") {
        dependsOn("classes")
        group = "ririfa"
        description = "Creates a relocated fat jar containing shadedAPI dependencies"
        archiveClassifier.set("fat")
        configurations.add(project.configurations.getByName("akkaraAPI"))
        from(sourceSets.main.get().output)

        doFirst {
            val akkara = project.configurations.getByName("akkaraAPI")
            val artifacts = try {
                akkara.resolvedConfiguration.resolvedArtifacts
            } catch (e: Exception) {
                logger.warn("Could not resolve shadedAPI: ${e.message}")
                emptySet<ResolvedArtifact>()
            }

            artifacts.forEach { artifact ->
                val moduleName = artifact.moduleVersion.id.name.replace("-", "_")
                val jarFile = artifact.file

                val classPackages = JarFile(jarFile).use { jar ->
                    jar.entries().asSequence()
                        .filter { it.name.endsWith(".class") && !it.name.startsWith("META-INF") }
                        .mapNotNull { entry ->
                            entry.name
                                .replace('/', '.')
                                .removeSuffix(".class")
                                .substringBeforeLast('.', "")
                        }
                        .toSet()
                }

                if (classPackages.any { it.startsWith("net.ririfa") }) {
                    logger.lifecycle("Skipping relocation for ${artifact.moduleVersion.id} (self package detected)")
                    return@forEach
                }

                classPackages.forEach cp@{ pkg ->
                    if (pkg.isBlank()) return@cp
                    val relocated = "net.ririfa.shaded.$moduleName.${pkg.replace('.', '_')}"
                    logger.lifecycle("Relocating $pkg → $relocated")
                    relocate(pkg, relocated)
                }
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

                artifact(tasks.named<Jar>("plainJar"))
                artifact(tasks.named<ShadowJar>("relocatedFatJar"))
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