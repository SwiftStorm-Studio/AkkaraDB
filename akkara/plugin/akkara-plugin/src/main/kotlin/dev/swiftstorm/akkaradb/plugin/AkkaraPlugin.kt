package dev.swiftstorm.akkaradb.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.api.provider.Provider
import org.jetbrains.kotlin.gradle.plugin.*
import java.net.URL
import javax.xml.parsers.DocumentBuilderFactory

private var akkaraLibraryVersion: String? = null

@Suppress("unused")
fun DependencyHandler.akkara(
    version: String,
    scope: String = "implementation"
) {
    if (version.isBlank()) error("[akkara] Version must not be blank")
    akkaraLibraryVersion = version
    add(scope, "dev.swiftstorm:akkaradb:$version")
}

class AkkaraGradlePlugin : Plugin<Project>, KotlinCompilerPluginSupportPlugin {

    private fun fetchLatestCompilerVersion(): String {
        val metadataUrl =
            "https://repo.ririfa.net/repository/maven-public/dev/swiftstorm/akkara-compiler/maven-metadata.xml"
        return try {
            val xml = URL(metadataUrl).openStream().use { it.reader(Charsets.UTF_8).readText() }
            val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(xml.byteInputStream())

            doc.documentElement.normalize()

            doc.getElementsByTagName("latest").item(0)?.textContent
                ?: error("No <latest> tag")
        } catch (e: Exception) {
            throw RuntimeException("Failed to fetch latest Akkara compiler version", e)
        }
    }

    override fun apply(target: Project) {}

    override fun isApplicable(kotlinCompilation: KotlinCompilation<*>): Boolean =
        kotlinCompilation.target.platformType == KotlinPlatformType.jvm &&
                kotlinCompilation.target.project.plugins.hasPlugin(AkkaraGradlePlugin::class.java)

    override fun getCompilerPluginId(): String =
        "dev.swiftstorm.akkaradb.compiler"

    override fun getPluginArtifact(): SubpluginArtifact {
        val project = getAppliedProjectOrThrow()

        val versionProvider: Provider<String> =
            project.providers.provider { fetchLatestCompilerVersion() }

        return SubpluginArtifact(
            groupId = "dev.swiftstorm",
            artifactId = "akkara-compiler",
            version = versionProvider.get()
        )
    }

    override fun applyToCompilation(kotlinCompilation: KotlinCompilation<*>)
            : Provider<List<SubpluginOption>> {
        val providers = kotlinCompilation.target.project.providers

        return providers.provider {
            akkaraLibraryVersion
                ?.let { listOf(SubpluginOption("akkaraVersion", it)) }
                ?: emptyList()
        }
    }

    private fun getAppliedProjectOrThrow(): Project =
        Project::class.java.getDeclaredField("project").let { field ->
            field.isAccessible = true
            field.get(this) as Project
        }
}
