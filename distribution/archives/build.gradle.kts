import org.apache.tools.ant.filters.ConcatFilter
import java.nio.file.Path
import java.util.*

plugins {
    id("elasticsearch.internal-distribution-archive-setup")
    id("elasticsearch.distro")
}

val copyDistributionDefaults by tasks.registering(Sync::class) {
    into(layout.buildDirectory.dir("contents"))
}

project(":modules").subprojects.forEach { distro.copyModule(copyDistributionDefaults, it) }

val serverCli by configurations.creating
val bundledPlugins by configurations.creating {
    attributes {
        attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE)
    }
}
val xpackTemplateResources by configurations.creating
val additionalLibJars by configurations.creating

dependencies {
    serverCli(project(":distribution:tools:serverless-server-cli"))
    bundledPlugins("org.elasticsearch.plugin:analysis-icu")
    bundledPlugins("org.elasticsearch.plugin:analysis-nori")
    bundledPlugins("org.elasticsearch.plugin:analysis-smartcn")
    bundledPlugins("org.elasticsearch.plugin:analysis-ukrainian")
    bundledPlugins("org.elasticsearch.plugin:analysis-kuromoji")
    bundledPlugins("org.elasticsearch.plugin:analysis-phonetic")
    bundledPlugins("org.elasticsearch.plugin:analysis-stempel")
    xpackTemplateResources(project(":libs:serverless-xpack-template-resources"))
    additionalLibJars(project(":libs:serverless-build-info"))
    additionalLibJars(project(":libs:serverless-shared-constants"))
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
                        // Strip the version from the file path of the upstream distribution
                        path = "elasticsearch/${Path.of(path).run { subpath(1, count())}}"
                    }
                    filesMatching("*/config/jvm.options") {
                        // Add serverless jvm options to default distribution jvm.options file
                        filter(mapOf("append" to file("src/additional-jvm.options")), ConcatFilter::class.java)
                    }
                    exclude("*/bin/elasticsearch")
                    exclude("*/bin/elasticsearch.bat")
                    exclude("*/modules/searchable-snapshots")
                    exclude("*/modules/transform")
                    exclude("*/modules/x-pack-voting-only-node")
                    exclude("*/modules/x-pack-monitoring")
                    exclude("*/modules/x-pack-ilm")
                    exclude("*/modules/x-pack-rollup")
                    exclude("*/modules/x-pack-shutdown")
                    exclude("*/modules/x-pack-watcher")
                    exclude("*/modules/snapshot-based-recoveries")
                    // this jar is redefined, see libs/serverless-xpack-template-resources
                    exclude("*/modules/x-pack-core/x-pack-template-resources*.jar")
                    exclude("*/modules/x-pack-esql")
                    exclude("*/modules/rest-root")
                    exclude("*/modules/health-shards-availability")
                    includeEmptyDirs = false
                }
                into("elasticsearch") {
                    from(copyDistributionDefaults)
                    from((project.ext["logsDir"] as File).parent)
                    from((project.extensions["pluginsDir"] as File).parent)
                    into("bin") {
                        from(if (name.contains("windows")) "src/bin/elasticsearch.bat" else "src/bin/elasticsearch") {
                            fileMode = 0b111_101_101
                        }
                    }
                    into("config") {
                        from((project.extensions["jvmOptionsDir"] as File).parent)
                        from("src/serverless-default-settings.yml")
                    }
                    into("lib/tools/serverless-server-cli") {
                        from(serverCli)
                    }
                    into("lib") {
                        from(additionalLibJars)
                    }
                    into("modules") {
                        // Bundle analysis language plugins as modules
                        from(bundledPlugins) {
                            eachFile {
                                // Determine owning plugin and rewrite path
                                val filePath = Path.of(path).run { subpath(2, count()) }
                                val pluginName = file.toPath().run { subpath(0, count() - filePath.count() )}.fileName
                                path = "elasticsearch/modules/${pluginName}/${filePath}"
                            }
                        }
                        into("x-pack-core") {
                            from(xpackTemplateResources)
                        }
                    }
                }
            }
        }
    }
}

fun archiveToSubprojectName(taskName: String): String = taskName.replace("[A-Z]".toRegex(), "-$0").lowercase(Locale.getDefault())

tasks.withType<AbstractCopyTask>().configureEach {
    inputs.file("src/additional-jvm.options")
}
