import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.artifacts.dsl.DependencyHandler

/**
 * Returns a dependency notation for the given x-pack plugin project.
 *
 * @param name The name of the x-pack project
 */
fun DependencyHandler.xpackModule(name: String): String = "org.elasticsearch.plugin:${name}"

/**
 * Creates a dependency on the test source set artifact for the given module dependency.
 *
 * @param dependency The module dependency notation
 */
fun DependencyHandler.testArtifact(dependencyNotation: String): Dependency {
    val dependency = this.create(dependencyNotation) as ModuleDependency
    dependency.capabilities { this.requireCapability("org.elasticsearch.gradle:${dependencyNotation.split(":")[1]}-test-artifacts") }

    return dependency
}
