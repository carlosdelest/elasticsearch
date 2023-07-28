
import org.elasticsearch.gradle.transform.UnzipTransform

description = "Overrides of xpack template resources"

val shouldFilter = Attribute.of("shouldFilter", Boolean::class.javaObjectType)

configurations {
    create("upstreamResources") {
        attributes.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
        attributes.attribute(shouldFilter, true)
    }
}

dependencies {
    registerTransform(UnzipTransform::class) {
        from.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.JAR_TYPE)
            .attribute(shouldFilter, true)
        to.attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
    }

    "upstreamResources"(xpackModule("template-resources")) {
        isTransitive = false
    }
}

tasks.jar {
    // files local to this project are already added as a "from" location to this copy spec, so
    // having an exlude duplicate strategy will skip any resource files from the upstream
    // jar file that also exist here
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations["upstreamResources"])
}
