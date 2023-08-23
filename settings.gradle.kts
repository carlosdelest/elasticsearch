pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }

    includeBuild("elasticsearch/build-conventions")
    includeBuild("elasticsearch/build-tools")
    includeBuild("elasticsearch/build-tools-internal")
}

plugins {
    id("com.gradle.enterprise") version ("3.13.1")
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
include(":modules:stateless")
include(":modules:secure-settings")
include(":modules:serverless-autoscaling")
include(":modules:serverless-datastream-lifecycle")
include(":modules:serverless-enterprise-search")
include(":modules:serverless-fleet")
include(":modules:serverless-kibana")
include(":modules:serverless-license")
include(":modules:serverless-ml")
include(":modules:serverless-rest-controller")
include(":modules:serverless-rest-root")
include(":modules:serverless-search")
include(":modules:serverless-security")
include(":modules:serverless-sigterm")
include(":modules:serverless-master-failover")
include(":modules:serverless-transform")
include(":modules:serverless-metering")
include(":modules:serverless-api-filtering")


/*
 * Extraneous libs
 */
include(":libs:serverless-build-info")
include(":libs:serverless-metrics")
include(":libs:serverless-shared-constants")
include(":libs:serverless-xpack-template-resources")

/*
 * Distribution projects
 */
include(":distribution:archives:darwin-tar")
include(":distribution:archives:darwin-aarch64-tar")
include(":distribution:archives:integ-test-zip")
include(":distribution:archives:linux-tar")
include(":distribution:archives:linux-aarch64-tar")
include(":distribution:archives:windows-zip")
include(":distribution:bwc")
include(":distribution:docker")

/*
 * Distribution tools
 */
include(":distribution:tools:serverless-server-cli")

/*
 * QA Projects
 */
include(":qa:core-rest-test")
include(":qa:rolling-upgrade")
include(":qa:sigterm-shutdown")
include(":qa:x-pack-rest-test")
include(":qa:e2e-test")
include(":qa:master-failover")

/*
 * Test Framework
 */
include(":serverless-test-framework")
