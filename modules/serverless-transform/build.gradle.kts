plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "serverless-transform"
    description = "Serverless Transform module for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.transform.ServerlessTransformPlugin"
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
    implementation(xpackModule("transform"))
    testImplementation(xpackModule("core"))
}
