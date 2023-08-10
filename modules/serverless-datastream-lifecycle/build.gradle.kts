plugins {
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "serverless-datastream-lifecycle"
    description = "Data Stream Lifecycle for Elasticsearch"
    classname = "co.elastic.elasticsearch.lifecycle.serverless.ServerlessDataStreamLifecyclePlugin"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

tasks {
    javaRestTest {
        usesDefaultDistribution()
    }
}

dependencies {
    compileOnly("org.elasticsearch:server")

    javaRestTestImplementation(testArtifact("org.elasticsearch:server"))
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    javaRestTestImplementation(testArtifact(xpackModule("core")))
}
