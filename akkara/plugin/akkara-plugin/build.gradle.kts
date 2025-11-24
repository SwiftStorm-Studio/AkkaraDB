plugins {
    `java-gradle-plugin`
    `maven-publish`
}

gradlePlugin {
    plugins {
        create("akkaraPlugin") {
            id = "dev.swiftstorm.akkaradb-plugin"
            implementationClass = "dev.swiftstorm.akkaradb.plugin.AkkaraGradlePlugin"
        }
    }
}

publishing {
    repositories {
        maven {
            val isSnapshot = version.toString().endsWith("SNAPSHOT")
            url = uri(
                if (isSnapshot)
                    "https://repo.swiftstorm.dev/maven2-snap"
                else
                    "https://repo.swiftstorm.dev/maven2-rel"
            )
            credentials {
                username = findProperty("nxUN") as String?
                password = findProperty("nxPW") as String?
            }
        }
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Version" to project.version.toString()
        )
    }
}