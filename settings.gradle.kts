pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("com.gradle.enterprise") version ("3.11.4")
}

dependencyResolutionManagement {
    versionCatalogs {
        create("buildLibs") {
            from(files("elasticsearch/gradle/build.versions.toml"))
        }
    }
}

rootProject.name = "elasticsearch-serverless"

includeBuild("serverless-build-tools")
includeBuild("elasticsearch")

/*
 * Check to ensure git submodules have been initialized
 */
if (file("elasticsearch/.git").exists() == false) {
    throw GradleException("The 'elasticsearch' submodule has not been initialized. " +
            "Run 'git submodule update --init' to setup your workspace.")
}

/*
 * Modules
 */
include(":modules:ml-serverless")
include(":modules:stateless")
include(":modules:secure-settings")
include(":modules:serverless-license")

/*
 * Distribution projects
 */
include(":distribution:archives:darwin-tar")
include(":distribution:archives:darwin-aarch64-tar")
include(":distribution:archives:integ-test-zip")
include(":distribution:archives:linux-tar")
include(":distribution:archives:linux-aarch64-tar")
include(":distribution:archives:windows-zip")
include(":distribution:docker")

/*
 * Distribution tools
 */
include(":distribution:tools:serverless-server-cli")

/*
 * QA Projects
 */
include(":qa:core-rest-test")
include(":qa:x-pack-rest-test")
