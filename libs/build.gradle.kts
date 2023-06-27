plugins {
    id("elasticsearch.internal-es-plugin") apply(false)
}

subprojects {
    apply(plugin = "elasticsearch.build")

    // Add standard dependencies to all modules
    dependencies {
        add("compileOnly", "org.elasticsearch:server")
        add("testImplementation", "org.elasticsearch.test:framework")
    }
}
