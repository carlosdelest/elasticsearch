import org.elasticsearch.gradle.internal.toolchain.AdoptiumJdkToolchainResolver
import org.elasticsearch.gradle.internal.toolchain.ArchivedOracleJdkToolchainResolver
import org.elasticsearch.gradle.internal.toolchain.OracleOpenJdkToolchainResolver

pluginManagement {
    repositories {
        // for the custom shadow plugin version we currently use for 8.8 compatibility
        maven {
            name = "jitpack"
            url = uri("https://jitpack.io")
        }
        mavenCentral()
        gradlePluginPortal()
    }

    includeBuild("elasticsearch/build-conventions")
    includeBuild("elasticsearch/build-tools")
    includeBuild("elasticsearch/build-tools-internal")
}

plugins {
    id("com.gradle.develocity") version ("3.19.2")
    id("elasticsearch.java-toolchain")
}

develocity {
    buildScan {
        publishing.onlyIf { server.isPresent }
    }
}

toolchainManagement {
    jvm {
        javaRepositories {
            repository("bundledOracleOpendJdk") {
                resolverClass = OracleOpenJdkToolchainResolver::class.java
            }
            repository("adoptiumJdks") {
                resolverClass = AdoptiumJdkToolchainResolver::class.java
            }
            repository("archivedOracleJdks") {
                resolverClass = ArchivedOracleJdkToolchainResolver::class.java
            }
        }
    }
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
includeBuild("elasticsearch/build-tools-internal")

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
include(":modules:serverless-cross-project")
include(":modules:serverless-datastream")
include(":modules:serverless-downsample")
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
include(":modules:serverless-multi-project")
include(":modules:serverless-transform")
include(":modules:serverless-metering")
include(":modules:serverless-api-filtering")
include(":modules:serverless-health-shards-availability")
include(":modules:serverless-snapshots")
include(":modules:serverless-no-wait-for-active-shards")
include(":modules:serverless-logs-essentials")

// Test fixtures
include(":serverless-test-fixture")
include(":serverless-test-fixture:uiam-fixture")

/*
 * Extraneous libs
 */
include(":libs:serverless-build-info")
include(":libs:serverless-metrics")
include(":libs:serverless-shared-constants")
include(":libs:serverless-stateless-api")
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
include(":distribution:tools:serverless-wipe-data-cli")

/*
 * QA Projects
 */
include(":qa:core-rest-test")
include(":qa:rolling-upgrade")
include(":qa:sigterm-shutdown")
include(":qa:x-pack-rest-test")
include(":qa:e2e-test")
include(":qa:multi-project-auth-test")
include(":qa:master-failover")
include(":qa:sso-authentication")
include(":qa:multi-project-smoke-test")

/*
 * Test Framework
 */
include(":serverless-test-framework")

/*
 * Benchmarks
 */
include(":serverless-benchmarks")


/*
 * Stateful ES QA Projects
 */
includeStatefulTests(":modules:aggregations", includeYamlTests = true, includeJavaTests = false)
includeStatefulTests(":modules:mapper-extras", includeYamlTests = true, includeJavaTests = false)
includeStatefulTests(":modules:data-streams", includeYamlTests = true, includeJavaTests = true)
includeStatefulTests(":modules:ingest-common", includeYamlTests = true, includeJavaTests = false)
includeStatefulTests(":test:external-modules:test-esql-heap-attack", includeYamlTests = false, includeJavaTests = true)
includeStatefulTests(":x-pack:plugin:eql:qa:rest", includeYamlTests = true, includeJavaTests = true)
includeStatefulTests(":x-pack:plugin:esql:qa:server:single-node", includeYamlTests = true, includeJavaTests = true)
includeStatefulTests(":x-pack:plugin:esql:qa:server:multi-node", includeYamlTests = true, includeJavaTests = true)
includeStatefulTests(":x-pack:plugin:esql:qa:server:mixed-cluster", includeYamlTests = true, includeJavaTests = true)
includeStatefulTests(":x-pack:plugin:sql:qa:server:single-node", includeYamlTests = false, includeJavaTests = true)
includeStatefulTests(":x-pack:plugin:downsample:qa:rest", includeYamlTests = true, includeJavaTests = false)
includeStatefulTests(":x-pack:plugin:fleet", includeYamlTests = true, includeJavaTests = false)

fun includeStatefulTests(projectPath: String, includeYamlTests: Boolean, includeJavaTests: Boolean) {
    include(":qa:stateful${projectPath}")
    gradle.projectsLoaded {
        rootProject.findProject(":qa:stateful${projectPath}")?.extra?.set("includeYamlTests", includeYamlTests)
        rootProject.findProject(":qa:stateful${projectPath}")?.extra?.set("includeJavaTests", includeJavaTests)
    }
}
