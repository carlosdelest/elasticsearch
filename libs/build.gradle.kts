plugins {
    id("elasticsearch.internal-es-plugin") apply(false)
}

subprojects {
    apply(plugin = "elasticsearch.build")
}
