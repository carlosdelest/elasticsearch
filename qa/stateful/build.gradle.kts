import org.elasticsearch.gradle.internal.test.rest.RestTestBasePlugin
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask

plugins {
    id("elasticsearch.internal-yaml-rest-test") apply (false)
    id("elasticsearch.internal-java-rest-test") apply (false)
}

val rootProjectPath: String = project.path

subprojects {
    val includeYamlTests = project.findProperty("includeYamlTests") as Boolean? ?: false
    val includeJavaTests = project.findProperty("includeJavaTests") as Boolean? ?: false

    if (includeYamlTests || includeJavaTests) {
        pluginManager.apply(RestTestBasePlugin::class)

        val projectPath = project.path.removePrefix(rootProjectPath)
        val artifactGroup =
            if (projectPath.contains(":qa:")) "org.elasticsearch" + projectPath.substring(
                ":x-pack".length,
                projectPath.lastIndexOf(":")
            ).replace(":", ".") else "org.elasticsearch.plugin"
        val artifactName = projectPath.split(":").last()
        val sourceSets = extensions.getByType<JavaPluginExtension>().sourceSets

        if (includeYamlTests) {
            apply(plugin = "elasticsearch.internal-yaml-rest-test")

            val yamlTests = configurations.create("yamlTests") { isTransitive = false }

            dependencies {
                "yamlTests"(testArtifact("${artifactGroup}:${artifactName}", "yamlRestTest"))
                "yamlRestTestImplementation"(testArtifact("${artifactGroup}:${artifactName}", "yamlRestTest"))
                "clusterPlugins"("${artifactGroup}:${artifactName}")
            }

            val unpackYamlRestTests by tasks.registering(Copy::class) {
                dependsOn(yamlTests)
                from({ zipTree(yamlTests.singleFile) })
                from(sourceSets.getByName("yamlRestTest").output)
                into(project.layout.buildDirectory.dir("yamlTests"))

                duplicatesStrategy = DuplicatesStrategy.INCLUDE
            }

            tasks {
                named<ProcessResources>("processYamlRestTestResources") {
                    dependsOn(yamlTests)
                    from({ zipTree(yamlTests.singleFile) }) {
                        include("rest-api-spec/**")
                    }
                }
                named<StandaloneRestIntegTestTask>("yamlRestTest") {
                    testClassesDirs = files(unpackYamlRestTests)

                    // Remove original source set to avoid jar hell
                    classpath -= sourceSets.getByName("yamlRestTest").output

                    // Replace the test jar with the exploded classes directory on the classpath.
                    // Some tests implicitly rely on loading classpath resources from a file path.
                    classpath -= files({ yamlTests.singleFile })
                    classpath += files(unpackYamlRestTests)
                }
            }
        }

        if (includeJavaTests) {
            apply(plugin = "elasticsearch.internal-java-rest-test")

            val javaTests = configurations.create("javaTests") { isTransitive = false }

            dependencies {
                "javaTests"(testArtifact("${artifactGroup}:${artifactName}", "javaRestTest"))
                "javaRestTestImplementation"(testArtifact("${artifactGroup}:${artifactName}", "javaRestTest"))
                "clusterPlugins"("${artifactGroup}:${artifactName}")
            }

            val unpackJavaRestTests by tasks.registering(Copy::class) {
                dependsOn(javaTests)
                from({ zipTree(javaTests.singleFile) })
                from(sourceSets.getByName("javaRestTest").output)
                into(project.layout.buildDirectory.dir("javaTests"))

                duplicatesStrategy = DuplicatesStrategy.INCLUDE
            }

            tasks {
                named<StandaloneRestIntegTestTask>("javaRestTest") {
                    testClassesDirs = files(unpackJavaRestTests)

                    // Remove original source set to avoid jar hell
                    classpath -= sourceSets.getByName("javaRestTest").output

                    // Replace the test jar with the exploded classes directory on the classpath.
                    // Some tests implicitly rely on loading classpath resources from a file path.
                    classpath -= files({ javaTests.singleFile })
                    classpath += files(unpackJavaRestTests)
                }
            }
        }

        tasks.withType<StandaloneRestIntegTestTask>().configureEach {
            systemProperty("tests.rest.cluster.username", "stateful_rest_test_admin")
            systemProperty("tests.rest.cluster.password", "x-pack-test-password")
        }
    }
}
