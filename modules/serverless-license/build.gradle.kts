plugins {
    id("elasticsearch.internal-yaml-rest-test")
}

esplugin {
    name = "serverless-license"
    description = "Serverless license module for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.license.ServerlessLicensePlugin"
    extendedPlugins = listOf("x-pack-core")
}

dependencies {
    yamlRestTestImplementation(project(":modules:stateless"))
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestRuntimeOnly(testArtifact(xpackModule("core")))
    compileOnly(xpackModule("core"))
}
