import org.elasticsearch.gradle.internal.ResolveAllDependencies

plugins {
    id("base")
    id("elasticsearch.global-build-info")
    id("elasticsearch.build-scan")
    id("elasticsearch.build-complete")
    id("elasticsearch.serverless-precommit")
    id("elasticsearch.docker-support")
    id("elasticsearch.internal-distribution-download")
    id("elasticsearch.local-distribution")
    id("elasticsearch.ide")
    id("elasticsearch.serverless-ide")
    id("elasticsearch.serverless-testing")
    id("elasticsearch.versions")
    id("elasticsearch.internal-testclusters")
    id("elasticsearch.serverless-run")
    id("elasticsearch.runtime-jdk-provision")
    id("elasticsearch.serverless-promotion-report")
}

repositories {
    // we add mavenCentral() to es serverless root project to fix
    // an issue with source download in the IDE when trying to resolve thirdparty
    // dependency sources declared by a git submodule (e.g. in this case elasticsearch)
    // where the source jar is resolved against the root project context and not by the subproject context.
    mavenCentral()
}

allprojects {
    repositories {
        maven {
            name = "opensaml"
            url = uri("https://artifactory.elstc.co/artifactory/shibboleth-releases/")
            content {
                // this repository *only* contains opensaml artifacts
                includeGroup("org.opensaml")
                includeGroup("net.shibboleth.utilities")
                includeGroup("net.shibboleth")
            }
        }
    }

    apply(plugin = "elasticsearch.formatting")

    tasks {
        register("resolveAllDependencies", ResolveAllDependencies::class) {
            configs = project.configurations
        }
    }
}
