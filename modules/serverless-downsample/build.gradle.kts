plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "serverless-downsample"
    description = "Serverless Downsample for Elasticsearch"
    classname = "co.elastic.elasticsearch.downsample.serverless.ServerlessDownsamplePlugin"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    implementation("org.elasticsearch:server")
    internalClusterTestImplementation(testArtifact(project(":modules:stateless"), "internalClusterTest"))

    javaRestTestImplementation(testArtifact("org.elasticsearch:server"))
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    javaRestTestImplementation(testArtifact(xpackModule("core")))
    internalClusterTestImplementation(xpackModule("downsample"))
    internalClusterTestImplementation(xpackModule("mapper-aggregate-metric"))
}
