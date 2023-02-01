import org.elasticsearch.gradle.VersionProperties

plugins {
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
}

allprojects {
    apply(plugin = "elasticsearch.formatting")

    version = VersionProperties.getElasticsearch()

    configurations {
        all {
            resolutionStrategy.dependencySubstitution {
                substitute(module("org.elasticsearch.distribution.integ-test-zip:elasticsearch")).using(variant(module("org.elasticsearch.distribution.integ-test-zip:integ-test-zip:${version}")) {
                    attributes {
                        attribute(Attribute.of("composite", Boolean::class.javaObjectType), true)
                    }
                })
            }
        }
    }
}
