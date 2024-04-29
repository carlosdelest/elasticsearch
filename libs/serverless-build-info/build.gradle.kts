
plugins {
    id("elasticsearch.internal-yaml-rest-test")
}

description = "Build info for the serverless repo introspected by Elasticsearch"

dependencies {
    compileOnly("org.elasticsearch:server")
    testImplementation("org.elasticsearch.test:framework")
}

restResources {
    restApi {
        include ("_common","nodes.info")
    }
}
