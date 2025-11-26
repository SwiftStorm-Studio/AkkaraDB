package dev.swiftstorm.akkaradb.plugin

import org.gradle.api.Project
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.api.provider.Provider
import org.jetbrains.kotlin.gradle.plugin.KotlinCompilation
import org.jetbrains.kotlin.gradle.plugin.KotlinCompilerPluginSupportPlugin
import org.jetbrains.kotlin.gradle.plugin.SubpluginArtifact
import org.jetbrains.kotlin.gradle.plugin.SubpluginOption
import java.net.URL
import javax.xml.parsers.DocumentBuilderFactory

@Suppress("unused")
fun DependencyHandler.akkara(
    version: String,
    scope: String = "implementation"
) {
    if (version.isBlank()) error("[akkara] Version must not be blank")
    add(scope, "dev.swiftstorm:akkaradb:$version")
}

@Suppress("unused")
class AkkaraGradlePlugin : KotlinCompilerPluginSupportPlugin {
    private lateinit var project: Project

    override fun apply(target: Project) {
        this.project = target
    }

    override fun applyToCompilation(kotlinCompilation: KotlinCompilation<*>): Provider<List<SubpluginOption>> {
        val providers = kotlinCompilation.target.project.providers
        return providers.provider { emptyList() }
    }

    override fun isApplicable(kotlinCompilation: KotlinCompilation<*>): Boolean = true

    override fun getCompilerPluginId(): String = "dev.swiftstorm.akkaradb.plugin.compiler"

    override fun getPluginArtifact(): SubpluginArtifact {
        val versionProvider: Provider<String> =
            project.providers.provider { fetchLatestCompilerVersion() }

        return SubpluginArtifact(
            groupId = "dev.swiftstorm",
            artifactId = "akkara-compiler",
            version = versionProvider.get()
        )
    }

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
}
