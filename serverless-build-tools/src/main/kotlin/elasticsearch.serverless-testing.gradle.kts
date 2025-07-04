import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask

allprojects {
    pluginManager.withPlugin("elasticsearch.internal-yaml-rest-test") {
        dependencies {
            add("yamlRestTestImplementation", "org.elasticsearch.test:yaml-rest-runner")
            add("yamlRestTestImplementation", "org.elasticsearch.test:test-clusters")
            add("yamlRestTestImplementation", project(":serverless-test-framework"))
            add("featuresMetadataDeps", "org.elasticsearch:server")
            add("defaultDistrofeaturesMetadataDeps", "org.elasticsearch:distribution")
        }

        tasks.withType<StandaloneRestIntegTestTask>().configureEach {
            // We should not require this for _ALL_ our yaml rest tests
            usesDefaultDistribution("to be triaged")
        }
    }
    pluginManager.withPlugin("elasticsearch.internal-java-rest-test") {
        dependencies {
            add("javaRestTestImplementation", "org.elasticsearch.test:yaml-rest-runner")
            add("javaRestTestImplementation", "org.elasticsearch.test:test-clusters")
            add("javaRestTestImplementation", project(":serverless-test-framework"))
            add("featuresMetadataDeps", "org.elasticsearch:server")
            add("defaultDistrofeaturesMetadataDeps", "org.elasticsearch:distribution")
        }

        tasks.withType<StandaloneRestIntegTestTask>().configureEach {
            // We should not require this for _ALL_ our yaml rest tests
            usesDefaultDistribution("to be triaged")
        }
    }
    pluginManager.withPlugin("elasticsearch.internal-distribution-download") {
        pluginManager.apply("elasticsearch.serverless-distribution-download")
    }
    pluginManager.withPlugin("elasticsearch.rest-resources") {
        dependencies {
            add("restSpec", "org.elasticsearch:rest-api-spec")
            add("restTestConfig", "org.elasticsearch:rest-api-spec")
            add("restXpackTestConfig", xpackModule("plugin"))
        }
    }
    pluginManager.withPlugin("elasticsearch.base-internal-es-plugin") {
        dependencies {
            add("featuresMetadataExtractor", "org.elasticsearch.test:metadata-extractor")
        }
    }
    pluginManager.withPlugin("elasticsearch.java-base") {
        dependencies {
            add("nativeLibs", "org.elasticsearch:native-libraries")
        }
    }
    pluginManager.withPlugin("elasticsearch.build") {
        dependencies {
            add("jarHell", "org.elasticsearch:core")
            add("jdkJarHell", "org.elasticsearch:core")
        }
    }

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
