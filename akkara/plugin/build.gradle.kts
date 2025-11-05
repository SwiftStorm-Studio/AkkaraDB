plugins {
    `java-gradle-plugin`
}

gradlePlugin {
    plugins {
        create("akkaraPlugin") {
            id = "dev.swiftstorm.akkaradb-plugin"
            implementationClass = "dev.swiftstorm.akkaradb.plugin.AkkaraGradlePlugin"
        }
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "Akkara Compiler/Gradle Plugin",
            "Implementation-Version" to project.version.toString()
        )
    }
}