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
        val projectPath = project.path.removePrefix(rootProjectPath)
        val artifactGroup =
            if (projectPath.contains(":qa:")) "org.elasticsearch" + projectPath.substring(
                ":x-pack".length,
                projectPath.lastIndexOf(":")
            ).replace(":", ".") else "org.elasticsearch.plugin"
        val artifactName = projectPath.split(":").last()

        if (includeYamlTests) {
            apply(plugin = "elasticsearch.internal-yaml-rest-test")

            val yamlTests = configurations.create("yamlTests") { isTransitive = false }

            dependencies {
                "yamlTests"(testArtifact("${artifactGroup}:${artifactName}", "yamlRestTest"))
                "yamlRestTestImplementation"(testArtifact("${artifactGroup}:${artifactName}", "yamlRestTest"))
            }

            val unpackYamlRestTests by tasks.registering(Copy::class) {
                dependsOn(yamlTests)
                from({ zipTree(yamlTests.singleFile) })
                into(project.layout.buildDirectory.dir("yamlTests"))
            }

            tasks {
                named<ProcessResources>("processYamlRestTestResources") {
                    from(unpackYamlRestTests) {
                        include("rest-api-spec/**")
                    }
                }
                named<StandaloneRestIntegTestTask>("yamlRestTest") {
                    testClassesDirs = files(unpackYamlRestTests)
                }
            }
        }

        if (includeJavaTests) {
            apply(plugin = "elasticsearch.internal-java-rest-test")

            val javaTests = configurations.create("javaTests") { isTransitive = false }

            dependencies {
                "javaTests"(testArtifact("${artifactGroup}:${artifactName}", "javaRestTest"))
                "javaRestTestImplementation"(testArtifact("${artifactGroup}:${artifactName}", "javaRestTest"))
            }

            val unpackJavaRestTests by tasks.registering(Copy::class) {
                dependsOn(javaTests)
                from({ zipTree(javaTests.singleFile) })
                into(project.layout.buildDirectory.dir("javaTests"))
            }

            tasks {
                named<StandaloneRestIntegTestTask>("javaRestTest") {
                    testClassesDirs = files(unpackJavaRestTests)

                    // Replace the test jar with the exploded classes directory on the classpath.
                    // Some tests implicitly rely on loading classpath resources from a file path.
                    classpath -= files({ javaTests.singleFile })
                    classpath += files(unpackJavaRestTests)
                }
            }
        }

        tasks.withType<StandaloneRestIntegTestTask>().configureEach {
            usesDefaultDistribution()
            systemProperty("tests.rest.cluster.username", "stateful_rest_test_admin")
            systemProperty("tests.rest.cluster.password", "x-pack-test-password")
        }
    }
}
