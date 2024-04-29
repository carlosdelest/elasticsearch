import groovy.lang.Closure
import org.elasticsearch.gradle.serverless.ServerlessDistributionDownloadPlugin
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.kotlin.dsl.extra

/**
 * Returns a dependency notation for the given x-pack plugin project.
 *
 * @param name The name of the x-pack project
 */
fun DependencyHandler.xpackModule(name: String): String = "org.elasticsearch.plugin:${name}"

/**
 * Creates a dependency on the test source set artifact for the given module dependency.
 *
 * @param dependencyNotation The module dependency notation
 */
fun DependencyHandler.testArtifact(dependencyNotation: String, sourceSet: String = "test"): Dependency {
    val dependency = this.create(dependencyNotation) as ModuleDependency
    return testArtifact(dependency, sourceSet)
}

/**
 * Creates a dependency on the test source set artifact for the given module dependency.
 *
 * @param dependency The module dependency
 */
fun DependencyHandler.testArtifact(dependency: ModuleDependency, sourceSet: String = "test"): Dependency {
    dependency.capabilities { this.requireCapability("org.elasticsearch.gradle:${dependency.name}-${sourceSet}-artifacts") }

    return dependency
}

fun StandaloneRestIntegTestTask.usesDefaultDistribution() {
    if (!this.extra.has("usingDefaultDistro")) {
        val closure = this.extra.get("usesDefaultDistribution") as Closure<*>
        closure.call(this)
        this.extra.set("usingDefaultDistro", true)
    }
}

fun StandaloneRestIntegTestTask.usesBwcDistribution() {
    val closure = this.extensions.extraProperties.get("usesBwcDistribution") as Closure<*>
    // Use a hard-coded dummy version to indicate a BWC distribution
    // This is handled by a custom resolver in ServerlessDistributionDownloadPlugin
    closure.call(ServerlessDistributionDownloadPlugin.SERVERLESS_BWC_VERSION)

    // Fetch the actual "stack" version of the BWC distribution and pass it into the tests as a system property
    val systemProperties = extensions.getByType(SystemPropertyCommandLineArgumentProvider::class.java)
    systemProperties.systemProperty("tests.serverless.bwc_stack_version") {
        // Later versions of IntelliJ resolve argument providers at configuration time, so we need to be lenient here
        project.tasks.getByPath(":distribution:bwc:checkoutBwcBranch").extra
            .runCatching { get("stackVersion") }.getOrElse { "" }
            .toString()
    }

    // Disable BWC tests
    onlyIf("disabled") { !System.getProperty("tests.upgrade.skip", "false").toBoolean() }
}
