plugins {
    id("elasticsearch.internal-distribution-archive-setup")
    id("elasticsearch.distro")
}

val copyDistributionDefaults by tasks.registering(Sync::class) {
    into("$buildDir/contents")
}

project(":modules").subprojects.forEach { distro.copyModule(copyDistributionDefaults, it) }

distribution_archives {
    create("windowsZip") {
        setArchiveClassifier("windows-x86_64")
    }

    create("darwinTar") {
        setArchiveClassifier("darwin-x86_64")
    }

    create("darwinAarch64Tar") {
        setArchiveClassifier("darwin-aarch64")
    }

    create("linuxAarch64Tar") {
        setArchiveClassifier("linux-aarch64")
    }

    create("linuxTar") {
        setArchiveClassifier("linux-x86_64")
    }

    all {
        val distroDependency = dependencies.create("org.elasticsearch.distribution.default:${archiveToSubprojectName(name)}")
        val upstreamDistro = configurations.detachedConfiguration(distroDependency).apply {
            attributes {
                attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
            }
        }

        content {
            copySpec {
                from(upstreamDistro)
                into("elasticsearch-${version}") {
                    from(copyDistributionDefaults)
                }
            }
        }
    }
}

fun archiveToSubprojectName(taskName: String): String = taskName.replace("[A-Z]".toRegex(), "-$0").toLowerCase()
