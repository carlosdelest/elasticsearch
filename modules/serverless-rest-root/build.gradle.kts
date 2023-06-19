import org.elasticsearch.gradle.internal.info.BuildParams

plugins {
    id("elasticsearch.internal-yaml-rest-test")
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "serverless-rest-root"
    description = "Adds HEAD and GET / endpoint to Elasticsearch serverless"
    classname = "co.elasticsearch.serverless.rest.root.ServerlessMainRestPlugin"
}

dependencies {
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestRuntimeOnly(testArtifact(xpackModule("core")))
    implementation("org.elasticsearch.plugin:rest-root")
    compileOnly(xpackModule("core"))
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

tasks {
    yamlRestTest {
        usesDefaultDistribution()
    }
    javaRestTest {
        usesDefaultDistribution()
    }
}

restResources {
    restApi {
        include ("_common","indices.exists_index_template","info", "ping")
    }
}

artifacts {
    restTests( File(projectDir, "src/yamlRestTest/resources/rest-api-spec/test"))
}

tasks.named("validateModule") { enabled = false }
