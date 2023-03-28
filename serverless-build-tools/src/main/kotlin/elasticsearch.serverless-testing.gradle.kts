allprojects {
    pluginManager.withPlugin("elasticsearch.internal-yaml-rest-test") {
        dependencies {
            add("yamlRestTestImplementation", "org.elasticsearch.test:yaml-rest-runner")
            add("yamlRestTestImplementation", "org.elasticsearch.test:test-clusters")
        }
    }
    pluginManager.withPlugin("elasticsearch.rest-resources") {
        dependencies {
            add("restSpec", "org.elasticsearch:rest-api-spec")
            add("restTestConfig", "org.elasticsearch:rest-api-spec")
            add("restXpackTestConfig", xpackModule("plugin"))
        }
    }
}
