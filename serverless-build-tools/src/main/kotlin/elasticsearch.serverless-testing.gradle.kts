allprojects {
    pluginManager.withPlugin("elasticsearch.internal-yaml-rest-test") {
        dependencies {
            add("yamlRestTestImplementation", "org.elasticsearch.test:yaml-rest-runner")
            add("yamlRestTestImplementation", "org.elasticsearch.test:test-clusters")
            add("yamlRestTestImplementation", project(":serverless-test-framework"))
            add("featuresMetadataDeps", "org.elasticsearch:server")
            add("defaultDistrofeaturesMetadataDeps", "org.elasticsearch:distribution")
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
            add("nativeLibs", "org.elasticsearch:elasticsearch-native-libraries")
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
