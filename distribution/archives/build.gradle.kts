import java.nio.file.Path

plugins {
    id("elasticsearch.internal-distribution-archive-setup")
    id("elasticsearch.distro")
}

val copyDistributionDefaults by tasks.registering(Sync::class) {
    into("$buildDir/contents")
}

project(":modules").subprojects.forEach { distro.copyModule(copyDistributionDefaults, it) }

val serverCli by configurations.creating
val bundledPlugins by configurations.creating {
    attributes {
        attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
    }
}

dependencies {
    serverCli(project(":distribution:tools:serverless-server-cli"))
    bundledPlugins("org.elasticsearch.plugin:analysis-icu")
    bundledPlugins("org.elasticsearch.plugin:analysis-nori")
    bundledPlugins("org.elasticsearch.plugin:analysis-smartcn")
    bundledPlugins("org.elasticsearch.plugin:analysis-ukrainian")
    bundledPlugins("org.elasticsearch.plugin:analysis-kuromoji")
    bundledPlugins("org.elasticsearch.plugin:analysis-phonetic")
    bundledPlugins("org.elasticsearch.plugin:analysis-stempel")
}

distribution_archives {
    create("integTestZip")
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
        val distroDependency =
            if (name == "integTestZip") {
                dependencies.create("org.elasticsearch.distribution.integ-test-zip:integ-test-zip")
            } else {
                dependencies.create("org.elasticsearch.distribution.default:${archiveToSubprojectName(name)}")
            }
        val upstreamDistro = configurations.detachedConfiguration(distroDependency).apply {
            attributes {
                attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
            }
        }

        content {
            copySpec {
                from(upstreamDistro) {
                    eachFile {
                        if (relativePath.segments.reversed()[1] == "bin" || relativePath.segments.reversed()[1] == "MacOS" || relativePath.segments.last() == "jspawnhelper") {
                            mode = 0b111_101_101
                        }
                    }
                    exclude("*/bin/elasticsearch")
                    exclude("*/bin/elasticsearch.bat")
                    exclude("*/modules/searchable-snapshots")
                    exclude("*/modules/transform")
                    exclude("*/modules/x-pack-voting-only-node")
                    exclude("*/modules/x-pack-shutdown")
                }
                into("elasticsearch-${version}") {
                    from(copyDistributionDefaults)
                    into("bin") {
                        from(if (name.contains("windows")) "src/bin/elasticsearch.bat" else "src/bin/elasticsearch") {
                            fileMode = 0b111_101_101
                        }
                    }
                    into("config") {
                        from("src/config")
                    }
                    into("lib/tools/serverless-server-cli") {
                        from(serverCli)
                    }
                    into("modules") {
                        // Bundle analysis language plugins as modules
                        from(bundledPlugins) {
                            eachFile {
                                // Determine owning plugin and rewrite path
                                val filePath = Path.of(path).run { subpath(2, count()) }
                                val pluginName = file.toPath().run { subpath(0, count() - filePath.count() )}.fileName
                                path = "elasticsearch-${version}/modules/${pluginName}/${filePath}"
                            }
                        }
                    }
                }
            }
        }
    }
}

fun archiveToSubprojectName(taskName: String): String = taskName.replace("[A-Z]".toRegex(), "-$0").toLowerCase()
