import org.elasticsearch.gradle.internal.info.BuildParams

plugins {
  id("elasticsearch.internal-yaml-rest-test")
}

esplugin {
  name = "serverless-rest-root"
  description = "Overrides root endpoint with serverless response"
  classname = "co.elastic.elasticsearch.serverless.restroot.ServerlessRestRootPlugin"
}

dependencies {
  compileOnly(project(":libs:serverless-shared-constants"))
  implementation("org.elasticsearch.plugin:rest-root")
  yamlRestTestImplementation(project(":libs:serverless-shared-constants"))
}

restResources {
  restApi {
    include ("_common","info")
  }
}
