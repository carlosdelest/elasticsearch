import org.elasticsearch.gradle.internal.info.BuildParams

plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "stateless"
    description = "Stateless module for Elasticsearch"
    classname = "co.elastic.elasticsearch.stateless.Stateless"
}

dependencies {
    compileOnly("org.elasticsearch.plugin:core")
    internalClusterTestImplementation("org.elasticsearch.plugin:core") {
        capabilities {
            requireCapabilities("org.elasticsearch.gradle:core-test-artifacts")
        }
    }
}

tasks {
    internalClusterTest {
        systemProperty("es.use_stateless", "true")
        enabled = BuildParams.isSnapshotBuild()
    }
}