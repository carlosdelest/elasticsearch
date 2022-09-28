pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("com.gradle.enterprise") version ("3.8.1")
}

rootProject.name = "elasticsearch-cloud"

includeBuild("elasticsearch")

include(":modules:stateless")

include(":distributions:archives:darwin-tar")
include(":distributions:archives:darwin-aarch64-tar")
