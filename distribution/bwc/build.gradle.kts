/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.OS
import org.elasticsearch.gradle.internal.BwcGitExtension
import org.elasticsearch.gradle.internal.InternalBwcGitPlugin
import org.gradle.configurationcache.extensions.capitalized
import java.util.*

apply {
    plugin(InternalBwcGitPlugin::class)
}

val checkoutDirectory = layout.buildDirectory.dir("checkout")
val bwcTag = providers.systemProperty("tests.bwc.tag").getOrElse("current_dev")
val remote = providers.systemProperty("tests.bwc.remote").getOrElse("git@github.com:elastic/elasticsearch-serverless.git")
val bwcTestingEnabled = !System.getProperty("tests.upgrade.skip", "false").toBoolean()

configure<BwcGitExtension> {
    checkoutDir = checkoutDirectory.map(Directory::getAsFile)
    bwcBranch = provider { "main" }
}
tasks {
    // Create a task for initializing elasticsearch git submodule
    val initSubmodule by registering(LoggedExec::class) {
        dependsOn("createClone")
        workingDir = checkoutDirectory.map(Directory::getAsFile)
        commandLine("git", "submodule", "update", "--init", "--reference", "${rootDir}/elasticsearch")
    }

    named("addRemote") {
        extra["remote"] = remote
    }

    named<LoggedExec>("fetchLatest") {
        dependsOn(initSubmodule)
        commandLine("git", "fetch", "--tags", "--prune", "--prune-tags", remote, "--force", "--recurse-submodules")
    }

    named("checkoutBwcBranch") {
        extra["refspec"] = bwcTag
        doLast {
            checkoutDirectory.get().file("elasticsearch/build-tools-internal/version.properties").asFile.bufferedReader()
                .use { reader ->
                    val version = Properties().apply { load(reader) }.getProperty("elasticsearch")
                    extra["stackVersion"] = version
                }
        }
    }
}

// Register tasks and artifacts for backward compatibility testing
project(":distribution:archives").subprojects.map(Project::getName).forEach { distributionProject ->
    val buildTaskName = "buildBwc" + distributionProject.split("-").joinToString(transform = String::capitalized, separator = "")
    val artifactDestination = checkoutDirectory.map { it.dir("distribution/archives/${distributionProject}/build/install") }
    val buildTaskProvider = tasks.register(buildTaskName, LoggedExec::class) {
        onlyIf("bwc testing disabled") { bwcTestingEnabled }
        if (bwcTestingEnabled) {
            dependsOn("checkoutBwcBranch")
        }
        workingDir = checkoutDirectory.map(Directory::getAsFile)
        indentingConsoleOutput = layout.buildDirectory.file("refspec").map { it.asFile.readText().substring(0, 7) }

        inputs.file(layout.buildDirectory.file("refspec"))
        outputs.dir(artifactDestination)

        if (OS.current() == OS.WINDOWS) {
            executable = "cmd"
            args("/C", "call", checkoutDirectory.map { it.file("gradlew").asFile.toString() })
        } else {
            executable = checkoutDirectory.map { it.file("gradlew").asFile.toString() }
        }

        args("-Dscan.tag.NESTED")

        if (gradle.startParameter.isBuildCacheEnabled) {
            args("--build-cache")
        }

        val buildCacheUrl = System.getProperty("org.elasticsearch.build.cache.url")
        if (buildCacheUrl != null) {
            args("-Dorg.elasticsearch.build.cache.url=$buildCacheUrl")
        }

        args(":distribution:archives:${distributionProject}:extractedAssemble")
    }
    val artifactConfiguration = configurations.create("bwc_${distributionProject}")
    artifacts.add(artifactConfiguration.name, artifactDestination) {
        name = "elasticsearch"
        builtBy(buildTaskProvider)
        type = "directory"
    }
}
