plugins {
    id("elasticsearch.build")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "metering"
    description = "Metering module for Serverless Elasticsearch"
    classname = "co.elastic.elasticsearch.metering.MeteringPlugin"
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
    implementation(project(":libs:serverless-metrics"))
    testImplementation(testArtifact("org.elasticsearch:server"))
    internalClusterTestImplementation("org.elasticsearch.plugin:ingest-common")
    internalClusterTestImplementation(testArtifact(project(":modules:stateless"), "internalClusterTest"))
    internalClusterTestImplementation("org.elasticsearch.plugin:data-streams")
    compileOnly(xpackModule("core"))
    compileOnly(project(":libs:serverless-shared-constants"))
    compileOnly(project(":libs:serverless-stateless-api"))
}
