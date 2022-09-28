subprojects {
    val elasticsearch by configurations.creating {
        attributes {
            attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
        }
    }

    dependencies {
        elasticsearch("org.elasticsearch.distribution.default:${project.name}")
    }

    tasks {
        register<Sync>("copyDistro") {
            from(elasticsearch)
            into("${buildDir}/distributions")
        }
    }
}
