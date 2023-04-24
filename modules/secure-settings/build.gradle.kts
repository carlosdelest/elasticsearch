plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "secure-settings"
    description = "Secure Settings module for Serverless Elasticsearch"
    classname = "co.elastic.elasticsearch.settings.secure.SecureSettings"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    testImplementation(testArtifact("org.elasticsearch:server"))
}

