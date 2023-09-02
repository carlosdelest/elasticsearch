plugins {
    id("elasticsearch.build")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-java-rest-test")
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

tasks {
    javaRestTest {
        usesDefaultDistribution()
    }
}

dependencies {
    implementation(project(":libs:serverless-metrics"))
    testImplementation(testArtifact("org.elasticsearch:server"))
    internalClusterTestImplementation("org.elasticsearch.plugin:ingest-common")
    compileOnly(project(":libs:serverless-shared-constants"))
}
