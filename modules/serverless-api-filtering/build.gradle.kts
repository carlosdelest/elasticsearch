plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-yaml-rest-test")
}

esplugin {
    name = "serverless-api-filtering"
    description = "Serverless API Filtering module for Elasticsearch"
    classname = "co.elastic.elasticsearch.api.filtering.ServerlessApiFilteringPlugin"
    extendedPlugins = listOf("x-pack-core")
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    yamlRestTestImplementation(project(":modules:stateless"))
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestImplementation(testArtifact(xpackModule("core")))
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("enrich"))
}
