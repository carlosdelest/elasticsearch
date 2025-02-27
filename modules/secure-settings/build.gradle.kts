plugins {
    id("elasticsearch.internal-cluster-test")
}

esplugin {
    name = "secure-settings"
    description = "Secure Settings module for Serverless Elasticsearch"
    classname = "co.elastic.elasticsearch.settings.secure.ServerlessSecureSettingsPlugin"
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
    testImplementation(project(":modules:serverless-multi-project"))
    testImplementation(xpackModule("core"))
}

