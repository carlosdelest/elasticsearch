esplugin {
    name = "serverless-datastream-lifecycle"
    description = "Data Stream Lifecycle for Elasticsearch"
    classname = "co.elastic.elasticsearch.lifecycle.serverless.ServerlessDataStreamLifecyclePlugin"
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
}
