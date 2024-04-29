import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.VersionProperties
import org.elasticsearch.gradle.internal.DockerBase
import org.elasticsearch.gradle.internal.docker.DockerBuildTask
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin
import org.elasticsearch.gradle.internal.docker.DockerSupportService
import org.elasticsearch.gradle.internal.info.BuildParams
import org.elasticsearch.gradle.util.GradleUtils

plugins {
    id("elasticsearch.internal-yaml-rest-test")
}

val dockerSource by configurations.creating {
    attributes {
        attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
    }
}
val aarch64DockerSource by configurations.creating {
    attributes {
        attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
    }
}
val versions = VersionProperties.getVersions()

dependencies {
    dockerSource(project(":distribution:archives:linux-tar"))
    aarch64DockerSource(project(":distribution:archives:linux-aarch64-tar"))
    yamlRestTestImplementation("org.testcontainers:testcontainers:1.17.6")
    yamlRestTestImplementation("org.slf4j:slf4j-api:1.7.36")
    yamlRestTestImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.19.0")
    yamlRestTestImplementation("org.apache.commons:commons-compress:1.22")
    yamlRestTestImplementation("org.rnorth.duct-tape:duct-tape:1.0.8")
    yamlRestTestImplementation("com.github.docker-java:docker-java-api:3.2.14")
    yamlRestTestImplementation("com.github.docker-java:docker-java-core:3.2.14")
    yamlRestTestImplementation("com.github.docker-java:docker-java-transport:3.2.14")
    yamlRestTestImplementation("com.github.docker-java:docker-java-transport-zerodep:3.2.14")
    yamlRestTestImplementation("com.github.docker-java:docker-java-transport-httpclient5:3.2.14")
    yamlRestTestImplementation("com.fasterxml.jackson.core:jackson-core:${versions["jackson"]}")
    yamlRestTestImplementation("com.fasterxml.jackson.core:jackson-annotations:${versions["jackson"]}")
}

restResources {
    restTests {
        includeCore("indices.create")
    }
}

val dockerBuildTasks = Architecture.values().associateWith { architecture ->
    val baseName = if (architecture == Architecture.AARCH64) "Aarch64" else ""
    val upstreamContext = configurations.detachedConfiguration(dependencies.create("org.elasticsearch:docker")).apply {
        attributes {
            attribute(Attribute.of(Architecture::class.java), architecture)
            attribute(Attribute.of(DockerBase::class.java), DockerBase.DEFAULT)
        }
    }

    val transformTask = tasks.register<Sync>("transform${baseName}DockerContext") {
        into(layout.buildDirectory.dir("docker-context/${architecture.name}"))
        from(upstreamContext) {
            exclude("elasticsearch-*/**")
            eachFile {
                if (name == "Dockerfile") {
                    filter { contents ->
                        return@filter contents.replace("COPY elasticsearch-${VersionProperties.getElasticsearch()} .", "COPY elasticsearch .")
                    }
                }
            }
        }
        from(if (architecture == Architecture.AARCH64) aarch64DockerSource else dockerSource)
    }

    tasks.register<DockerBuildTask>("build${baseName}DockerImage") {
        dockerContext.fileProvider(transformTask.map { it.destinationDir })
        isNoCache = BuildParams.isCi()
        baseImages = arrayOf(DockerBase.DEFAULT.image)
        platforms = setOf(architecture.dockerPlatform)
        tags = if (architecture == Architecture.current()) {
            arrayOf("elasticsearch-serverless:${architecture.classifier}", "elasticsearch-serverless:latest")
        } else {
            arrayOf("elasticsearch-serverless:${architecture.classifier}")
        }

        onlyIf { isArchitectureSupported(architecture) }
    }
}

tasks {
    assemble {
        dependsOn(dockerBuildTasks.values)
    }
    copyRestApiSpecsTask {
        // This project doesn't have any tests of its own. It's just running the core elasticsearch rest tests.
        isSkipHasRestTestCheck = true
    }
    yamlRestTest {
        dependsOn(dockerBuildTasks[Architecture.current()])
        systemProperty("tests.rest.blacklist", listOf(
            "indices.create/20_synthetic_source/*"
        ).joinToString(","))
    }
}

fun isArchitectureSupported(architecture: Architecture): Boolean {
    val serviceProvider: Provider<DockerSupportService> =
        GradleUtils.getBuildService(project.gradle.sharedServices, DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)
    return serviceProvider.get().dockerAvailability.supportedArchitectures().contains(architecture)
}
