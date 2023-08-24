plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-yaml-rest-test")
}

esplugin {
    name = "serverless-transform"
    description = "Serverless Transform module for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.transform.ServerlessTransformPlugin"
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
    compileOnly(project(":modules:stateless"))

    implementation(xpackModule("transform"))
    testImplementation(xpackModule("core"))

    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestImplementation(testArtifact(xpackModule("core")))
}

tasks {
    yamlRestTest {
        usesDefaultDistribution()
    }
}
