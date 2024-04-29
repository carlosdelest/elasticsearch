plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
    id("elasticsearch.internal-yaml-rest-test")
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
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("ml"))

    testImplementation(testArtifact(xpackModule("core")))

    internalClusterTestImplementation(testArtifact(xpackModule("ml")))

    yamlRestTestImplementation(project(":modules:stateless"))
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestImplementation(testArtifact(xpackModule("core")))
    yamlRestTestImplementation(testArtifact(xpackModule("ml")))
}
