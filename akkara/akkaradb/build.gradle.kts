import java.net.HttpURLConnection
import java.net.URI

plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.shadow)
    `maven-publish`
}

java {
    withSourcesJar()
}

dependencies {
    implementation(project(":akkara-common"))
    implementation(project(":akkara-format-api"))
    implementation(project(":akkara-format-akk"))
    implementation(project(":akkara-engine"))
}

// ===== JAR strategy =====
// - shadowJar: fat (no classifier) => main artifact
// - jar: thin (classifier = "thin")  => not published as main
tasks.shadowJar {
    archiveClassifier.set("")

    dependencies {
        exclude(dependency("org.jetbrains.kotlin:.*"))
        exclude(dependency("org.jetbrains.kotlinx:kotlinx-.*"))
        exclude(dependency("org.jetbrains:annotations"))
    }

    exclude("META-INF/*.kotlin_module")
    exclude("META-INF/services/*kotlin*")
    exclude("META-INF/*kotlin*")
}
tasks.jar {
    archiveClassifier.set("thin")
}

// ===== Publish guard: skip if same version already exists on repo =====
tasks.withType<PublishToMavenRepository>().configureEach {
    onlyIf {
        val groupPath = project.group.toString().replace('.', '/')
        val artifactId = project.name
        val ver = project.version.toString()
        val repoRoot = if (ver.endsWith("SNAPSHOT")) {
            "https://repo.swiftstorm.dev/maven2-snap/"
        } else {
            "https://repo.swiftstorm.dev/maven2-rel/"
        }
        val artifactUrl = "${repoRoot}${groupPath}/$artifactId/$ver/$artifactId-$ver.jar"
        logger.lifecycle("Checking existence of artifact at: $artifactUrl")

        val exists = runCatching {
            (URI(artifactUrl).toURL().openConnection() as HttpURLConnection).let { c ->
                try {
                    c.requestMethod = "HEAD"
                    c.connectTimeout = 3000
                    c.readTimeout = 3000
                    c.responseCode == HttpURLConnection.HTTP_OK
                } finally {
                    c.disconnect()
                }
            }
        }.getOrDefault(false)

        if (exists) {
            logger.lifecycle("Artifact already exists at $artifactUrl, skipping publish.")
            false
        } else {
            logger.lifecycle("Artifact not found at $artifactUrl, proceeding with publish.")
            true
        }
    }
}

// ===== Publishing =====
publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            // Publish fat jar as the main component
            from(components["shadow"])
            // Attach sources and aggregated Dokka HTML
            artifact(tasks.named<Jar>("sourcesJar"))

            pom {
                name.set(project.name)
                description.set("Low-latency, crash-safe JVM KV store with WAL & stripe parity")
                url.set("https://github.com/SwiftStorm-Studio/AkkaraDB")
                licenses {
                    license {
                        name.set("LGPL 3.0")
                        url.set("https://www.gnu.org/licenses/lgpl-3.0.html")
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
                username = findProperty("nxUN")?.toString() ?: ""
                password = findProperty("nxPW")?.toString() ?: ""
            }
        }
    }
}