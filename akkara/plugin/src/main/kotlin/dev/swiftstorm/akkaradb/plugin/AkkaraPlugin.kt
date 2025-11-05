package dev.swiftstorm.akkaradb.plugin

import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.api.model.ObjectFactory
import org.gradle.api.plugins.ExtensionAware
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import org.jetbrains.kotlin.gradle.plugin.*
import org.jetbrains.kotlin.gradle.tasks.KotlinCompilationTask
import java.io.File
import javax.inject.Inject

/**
 * Holds a single version declared via the `akkara("<v>")` DSL.
 * The value is used for:
 *  - Adding dependency: "dev.swiftstorm:akkaradb:<v>"
 *  - Passing to the compiler plugin (published or dev fallback).
 */
open class AkkaraCollector @Inject constructor(objects: ObjectFactory) {
    val version: Property<String> = objects.property(String::class.java)
        .convention("") // empty means "not set"
}

/**
 * DSL entry point. Must be called exactly once.
 *
 * Usage:
 *   dependencies {
 *       akkara("1.2.3")
 *   }
 */
@Suppress("UNUSED")
fun DependencyHandler.akkara(version: String) {
    val extAware = this as ExtensionAware
    val collector = extAware.extensions.getByName("akkaraCollector") as AkkaraCollector
    val current = collector.version.orNull
    if (!current.isNullOrBlank()) {
        throw GradleException("[akkara] 'akkara(\"$version\")' was called but a version is already set: '$current'. Only one call is allowed.")
    }
    if (version.isBlank()) {
        throw GradleException("[akkara] Version must not be blank.")
    }
    collector.version.set(version)
    // Lock the value to prevent further mutation.
    collector.version.disallowChanges()
}

class AkkaraGradlePlugin : Plugin<Project>, KotlinCompilerPluginSupportPlugin {

    override fun apply(project: Project) {
        // Register collector on DependencyHandler for `dependencies { akkara("x") }`.
        val collector = AkkaraCollector(project.objects)
        (project.dependencies as ExtensionAware).extensions.add("akkaraCollector", collector)
        // Also expose the Property for other scripts if needed.
        project.extensions.add("akkaraVersion", collector.version)

        // When version becomes available, add library dependency once.
        project.afterEvaluate {
            val v = collector.version.orNull
            if (!v.isNullOrBlank()) addAkkaraLibraryDependency(project, v)
        }

        // Dev fallback: if no published version (manifest missing), inject -Xplugin and -P options.
        if (publishedVersion == null) {
            val selfJar = findSelfJar()
            if (selfJar != null) {
                project.logger.info("[akkara] Using dev fallback -Xplugin=${selfJar.absolutePath}")
                configureKotlinTasksForFallback(project, selfJar) {
                    val v = collector.version.orNull
                    if (v.isNullOrBlank()) emptyList()
                    else listOf("-Pplugin:${COMPILER_PLUGIN_ID}:akkaraVersion=$v")
                }
            } else {
                project.logger.warn("[akkara] Could not detect plugin JAR for fallback. IR plugin may not be applied in DEV mode.")
            }
        }
    }

    // ---- KotlinCompilerPluginSupportPlugin ----

    override fun isApplicable(kotlinCompilation: KotlinCompilation<*>): Boolean {
        // Apply only when a published version exists. Limit to JVM targets (incl. Android JVM & KMP JVM).
        return publishedVersion != null && kotlinCompilation.target.platformType == KotlinPlatformType.jvm
    }

    override fun getCompilerPluginId(): String = COMPILER_PLUGIN_ID

    override fun getPluginArtifact(): SubpluginArtifact = SubpluginArtifact(
        groupId = "dev.swiftstorm",
        artifactId = "akkara-plugin",
        version = publishedVersion ?: error("No version in manifest")
    )

    override fun applyToCompilation(kotlinCompilation: KotlinCompilation<*>)
            : Provider<List<SubpluginOption>> {
        val project = kotlinCompilation.target.project
        val versionProp = project.extensions.getByName("akkaraVersion") as Property<String>
        return versionProp.map { v ->
            if (v.isNullOrBlank()) emptyList()
            else listOf(SubpluginOption(key = "akkaraVersion", value = v))
        }
    }

    // ---- Helpers ----

    private val publishedVersion: String? = this::class.java.`package`.implementationVersion

    private fun findSelfJar(): File? = runCatching {
        val url = this::class.java.protectionDomain.codeSource.location
        val f = File(url.toURI())
        if (f.isFile && f.extension == "jar") f else null
    }.getOrNull()

    private fun configureKotlinTasksForFallback(
        project: Project,
        pluginJar: File,
        pOptionsSupplier: () -> List<String>
    ) {
        project.tasks.withType(KotlinCompilationTask::class.java).configureEach { task ->
            // -Xplugin=<self>
            val x = project.provider { listOf("-Xplugin=${pluginJar.absolutePath}") }
            // -P plugin:...:akkaraVersion=<v> if set
            val p = project.provider { pOptionsSupplier() }

            task.compilerOptions.freeCompilerArgs.addAll(x)
            task.compilerOptions.freeCompilerArgs.addAll(p)

            // De-duplicate just in case of repeated configuration
            task.compilerOptions.freeCompilerArgs.convention(
                task.compilerOptions.freeCompilerArgs.map { it.distinct() }
            )
        }
    }

    /**
     * Adds "dev.swiftstorm:akkaradb:<v>" to the most appropriate configurations.
     * Tries common Kotlin/JVM/Android/KMP config names; adds to those that exist.
     * Prefers "implementation" over "api". Adjust here if your consumers need 'api'.
     */
    private fun addAkkaraLibraryDependency(project: Project, v: String) {
        val notation = "dev.swiftstorm:akkaradb:$v"

        val conf = project.configurations.findByName("implementation")
        if (conf != null) {
            project.dependencies.add("implementation", notation)
        }
    }

    private companion object {
        const val COMPILER_PLUGIN_ID: String = "dev.swiftstorm.akkaradb.compiler"
    }
}
