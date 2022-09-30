pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("com.gradle.enterprise") version ("3.8.1")
}

dependencyResolutionManagement {
    versionCatalogs {
        create("buildLibs") {
            from(files("elasticsearch/gradle/build.versions.toml"))
        }
    }
}

rootProject.name = "elasticsearch-cloud"

includeBuild("cloud-build-tools")
includeBuild("elasticsearch")

/*
 * Modules
 */
include(":modules:stateless")

/*
 * Distribution projects
 */
include(":distribution:archives:darwin-tar")
include(":distribution:archives:darwin-aarch64-tar")
include(":distribution:archives:linux-tar")
include(":distribution:archives:linux-aarch64-tar")
include(":distribution:archives:windows-zip")
include(":distribution:docker")
