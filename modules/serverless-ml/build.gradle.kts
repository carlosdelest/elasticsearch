plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "serverless-ml"
    description = "Serverless Machine Learning module for Elasticsearch"
    classname = "co.elastic.elasticsearch.ml.serverless.ServerlessMachineLearningPlugin"
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
