import org.elasticsearch.gradle.Architecture
import org.elasticsearch.gradle.internal.DockerBase
import org.elasticsearch.gradle.internal.docker.DockerBuildTask
import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin
import org.elasticsearch.gradle.internal.docker.DockerSupportService
import org.elasticsearch.gradle.internal.info.BuildParams
import org.elasticsearch.gradle.util.GradleUtils

plugins {
    id("lifecycle-base")
}

val dockerSource by configurations.creating
val aarch64DockerSource by configurations.creating

dependencies {
    dockerSource(project(path = ":distribution:archives:linux-tar", configuration = "default"))
    aarch64DockerSource(project(path = ":distribution:archives:linux-aarch64-tar", configuration = "default"))
}

for (architecture in Architecture.values()) {
    val baseName = if (architecture == Architecture.AARCH64) "Aarch64" else ""
    val upstreamContext = configurations.detachedConfiguration(dependencies.create("org.elasticsearch:docker")).apply {
        attributes {
            attribute(Attribute.of(Architecture::class.java), architecture)
            attribute(Attribute.of(DockerBase::class.java), DockerBase.DEFAULT)
        }
    }

    val transformTask = tasks.register<Sync>("transform${baseName}DockerContext") {
        into("${buildDir}/docker-context/${architecture.name}")
        from(upstreamContext) {
            exclude("elasticsearch-*.tar.gz")
        }
        from(if (architecture == Architecture.AARCH64) aarch64DockerSource else dockerSource)
    }

    val buildTask = tasks.register<DockerBuildTask>("build${baseName}DockerImage") {
        dockerContext.fileProvider(transformTask.map { it.destinationDir })
        isNoCache = BuildParams.isCi()
        baseImages = arrayOf(DockerBase.DEFAULT.image)
        platform.set(architecture.dockerPlatform)
        tags = arrayOf("elasticsearch-serverless:${architecture.classifier}")

        onlyIf { isArchitectureSupported(architecture) }
    }

    tasks.named("assemble") {
        dependsOn(buildTask)
    }
}

fun isArchitectureSupported(architecture: Architecture): Boolean {
    val serviceProvider: Provider<DockerSupportService> = GradleUtils.getBuildService(project.gradle.sharedServices, DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME)
    return serviceProvider.get().dockerAvailability.supportedArchitectures().contains(architecture)
}
