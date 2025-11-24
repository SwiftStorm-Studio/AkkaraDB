plugins {
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
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