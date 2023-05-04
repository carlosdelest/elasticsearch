allprojects {
    pluginManager.withPlugin("elasticsearch.internal-yaml-rest-test") {
        dependencies {
            add("yamlRestTestImplementation", "org.elasticsearch.test:yaml-rest-runner")
            add("yamlRestTestImplementation", "org.elasticsearch.test:test-clusters")
            add("yamlRestTestImplementation", project(":serverless-test-framework"))
        }
    }
    pluginManager.withPlugin("elasticsearch.internal-java-rest-test") {
        dependencies {
            add("javaRestTestImplementation", "org.elasticsearch.test:yaml-rest-runner")
            add("javaRestTestImplementation", "org.elasticsearch.test:test-clusters")
            add("javaRestTestImplementation", project(":serverless-test-framework"))
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
}
