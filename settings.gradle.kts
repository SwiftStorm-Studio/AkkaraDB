pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

include("akkara-common")
project(":akkara-common").projectDir = file("akkara/common")

include("akkara-core")
project(":akkara-core").projectDir = file("akkara/core")

include("akkara-format-api")
project(":akkara-format-api").projectDir = file("akkara/format-api")

include("akkara-format-akk")
project(":akkara-format-akk").projectDir = file("akkara/format-akk")

include("akkara-format-cbor")
project(":akkara-format-cbor").projectDir = file("akkara/format-cbor")

include("akkara-java-api")
project(":akkara-java-api").projectDir = file("akkara/java-api")

include("akkara-replica")
project(":akkara-replica").projectDir = file("akkara/replica")

include("akkara-wal")
project(":akkara-wal").projectDir = file("akkara/wal")