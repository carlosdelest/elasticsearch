plugins {
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
    id("elasticsearch.internal-yaml-rest-test")
}

esplugin {
    name = "serverless-snapshots"
    description = "Serverless snapshots module for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.snapshots.ServerlessSnapshotsPlugin"
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
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("ml"))
    compileOnly("org.elasticsearch:server")
    compileOnly(project(":modules:stateless"))
}

tasks {
    yamlRestTest {
        usesDefaultDistribution()
    }
    javaRestTest {
        usesDefaultDistribution()
    }
}
