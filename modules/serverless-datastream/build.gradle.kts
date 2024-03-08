plugins {
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "serverless-datastream"
    description = "Data Stream for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.datastream.ServerlessDataStreamPlugin"
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
