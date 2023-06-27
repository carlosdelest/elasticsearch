plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "serverless-autoscaling"
    description = "Serverless autoscaling module for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.autoscaling.ServerlessAutoscalingPlugin"
    extendedPlugins = listOf("stateless")
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    compileOnly("org.elasticsearch:server")
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("ml"))
    compileOnly(project(":modules:stateless"))

    internalClusterTestImplementation(testArtifact(xpackModule("ml")))
}
