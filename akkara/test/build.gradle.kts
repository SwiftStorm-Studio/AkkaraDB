dependencies {
    testImplementation(kotlin("test"))
    testImplementation(project(":akkara-common"))
    testImplementation(project(":akkara-engine"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.13.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.13.1")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
    }
}
