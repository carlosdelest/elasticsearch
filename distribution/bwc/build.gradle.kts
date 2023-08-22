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

val checkoutDirectory = file("${buildDir}/checkout")
val bwcTag = providers.systemProperty("tests.bwc.tag").getOrElse("current_dev")
val remote = providers.systemProperty("tests.bwc.remote").getOrElse("git@github.com:elastic/elasticsearch-serverless.git")

configure<BwcGitExtension> {
    checkoutDir.set(checkoutDirectory)
    bwcBranch = provider { "main" }
}
tasks {
    // Create a task for initializing elasticsearch git submodule
    val initSubmodule by registering(LoggedExec::class) {
        dependsOn("createClone")
        workingDir.set(checkoutDirectory)
        commandLine("git", "submodule", "update", "--init", "--reference", "${rootDir}/elasticsearch")
    }

    named("addRemote") {
        extra.set("remote", remote)
    }

    named("fetchLatest") {
        dependsOn(initSubmodule)
    }

    named("checkoutBwcBranch") {
        extra.set("refspec", bwcTag)
        doLast {
            File("${checkoutDirectory}/elasticsearch/build-tools-internal/version.properties").bufferedReader().use { reader ->
                val version = Properties().apply { load(reader) }.getProperty("elasticsearch")
                extra.set("stackVersion", version)
            }
        }
    }
}

// Register tasks and artifacts for backward compatibility testing
project(":distribution:archives").subprojects.map(Project::getName).forEach { distributionProject ->
    val buildTaskName = "buildBwc" + distributionProject.split("-").joinToString(transform = String::capitalized, separator = "")
    val artifactDestination = "${checkoutDirectory}/distribution/archives/${distributionProject}/build/install"
    val buildTaskProvider = tasks.register(buildTaskName, LoggedExec::class) {
        dependsOn("checkoutBwcBranch")
        workingDir.set(checkoutDirectory)
        indentingConsoleOutput.set(provider { project.file("${buildDir}/refspec").readText().substring(0, 7) })

        inputs.file("${buildDir}/refspec")
        outputs.dir(artifactDestination)

        if (OS.current() == OS.WINDOWS) {
            executable.set("cmd")
            args("/C", "call", File(checkoutDirectory, "gradlew").toString())
        } else {
            executable.set(File(checkoutDirectory, "gradlew").toString())
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
    artifacts.add(artifactConfiguration.name, File(artifactDestination)) {
        name = "elasticsearch"
        builtBy(buildTaskProvider)
        type = "directory"
    }
}
