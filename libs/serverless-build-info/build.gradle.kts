
plugins {
    id("elasticsearch.internal-yaml-rest-test")
}

description = "Build info for the serverless repo introspected by Elasticsearch"

dependencies {
    compileOnly("org.elasticsearch:server")
}

tasks {
    yamlRestTest {
        usesDefaultDistribution()
    }
}

restResources {
    restApi {
        include ("_common","nodes.info")
    }
}
