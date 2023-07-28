description = "Metrics accessor library"

dependencies {
    compileOnly("org.elasticsearch:server")
    testImplementation("org.elasticsearch.test:framework")
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}
