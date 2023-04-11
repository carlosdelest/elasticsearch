plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "ml-serverless"
    description = "Machine Learning Serverless module for Elasticsearch"
    classname = "co.elastic.elasticsearch.ml.serverless.MachineLearningServerless"
    extendedPlugins = listOf("x-pack-ml")
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    compileOnly(xpackModule("ml"))

    internalClusterTestImplementation(testArtifact(xpackModule("ml")))
}
