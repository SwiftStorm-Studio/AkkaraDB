pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

fun safeInclude(name: String, path: String) {
    val dir = file(path)
    if (dir.exists()) {
        include(name)
        project(":$name").projectDir = dir
    }
}

safeInclude("akkaradb", "akkara/akkaradb")
safeInclude("akkara-test", "akkara/test")
safeInclude("akkara-common", "akkara/common")
safeInclude("akkara-engine", "akkara/engine")
safeInclude("akkara-plugin", "akkara/plugin/akkara-plugin")
safeInclude("akkara-compiler", "akkara/plugin/akkara-compiler")
safeInclude("akkara-format-api", "akkara/format-api")
safeInclude("akkara-format-akk", "akkara/format-akk")
safeInclude("akkara-cli", "akkara/cli")