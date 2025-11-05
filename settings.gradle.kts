pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

include("akkara-test")
project(":akkara-test").projectDir = file("akkara/test")

include("akkara-common")
project(":akkara-common").projectDir = file("akkara/common")

include("akkara-engine")
project(":akkara-engine").projectDir = file("akkara/engine")

include(":akkara-plugin")
project(":akkara-plugin").projectDir = file("akkara/plugin")

include("akkara-format-api")
project(":akkara-format-api").projectDir = file("akkara/format-api")

include("akkara-format-akk")
project(":akkara-format-akk").projectDir = file("akkara/format-akk")

include("akkara-cli")
project(":akkara-cli").projectDir = file("akkara/cli")