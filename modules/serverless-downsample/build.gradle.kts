plugins {
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

    javaRestTestImplementation(testArtifact("org.elasticsearch:server"))
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    javaRestTestImplementation(testArtifact(xpackModule("core")))
}
