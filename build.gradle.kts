import org.elasticsearch.gradle.VersionProperties

plugins {
    id("elasticsearch.global-build-info")
    id("elasticsearch.build-scan")
}

allprojects {
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
