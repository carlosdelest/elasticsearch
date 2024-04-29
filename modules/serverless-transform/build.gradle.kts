plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-yaml-rest-test")
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
    compileOnly(xpackModule("core"))

    implementation(xpackModule("transform"))

    testImplementation(testArtifact(xpackModule("core")))

    yamlRestTestImplementation(project(":modules:stateless"))
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestImplementation(testArtifact(xpackModule("core")))
}
