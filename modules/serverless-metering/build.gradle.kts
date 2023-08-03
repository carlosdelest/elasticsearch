plugins {
    id("elasticsearch.build")
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "metering"
    description = "Metering module for Serverless Elasticsearch"
    classname = "co.elastic.elasticsearch.metering.MeteringPlugin"
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

}
