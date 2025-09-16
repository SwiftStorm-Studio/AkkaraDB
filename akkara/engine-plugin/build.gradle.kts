plugins {
    alias(libs.plugins.kotlin)
}

dependencies {
    compileOnly(kotlin("compiler-embeddable"))
    compileOnly(kotlin("stdlib"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}

kotlin {
    jvmToolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "akkara-engine-plugin",
            "Implementation-Version" to "0.1.0"
        )
    }
    from("src/main/resources")
}
