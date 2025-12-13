tasks.test {
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
    }
    maxHeapSize = "512M"
}

application {
    mainClass.set("dev.swiftstorm.akkaradb.test.AkkaraDBBenchmarkKt")
}