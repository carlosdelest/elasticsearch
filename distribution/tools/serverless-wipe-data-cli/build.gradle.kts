import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask

/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

plugins {
    id("elasticsearch.java")
}

dependencies {
    api("com.amazonaws:aws-java-sdk-s3:1.12.684")
    api("com.amazonaws:aws-java-sdk-core:1.12.684")

    // aws-java-sdk-(s3|core) dependencies, recursively
    // (in order of things throwing java.lang.NoClassDefFoundError if you don't have them)
    api("commons-logging:commons-logging:1.2")
    api("com.fasterxml.jackson.core:jackson-databind:2.12.7.1")
    api("com.fasterxml.jackson.core:jackson-core:2.12.7")
    api("com.fasterxml.jackson.core:jackson-annotations:2.12.7")
    api("org.apache.httpcomponents:httpclient:4.5.14")
    api("org.apache.httpcomponents:httpcore:4.4.13")
    api("joda-time:joda-time:2.8.1")

    // com.amazonaws.util.Base64 complains about performance if you don't have jaxb available
    api("javax.xml.bind:jaxb-api:2.2.2")

    compileOnly("org.elasticsearch:server")
    compileOnly("org.elasticsearch:elasticsearch-cli")
    compileOnly("org.elasticsearch:server-cli")

    // serverless libs are placed on the server classpath, so they are already present at runtime
    compileOnly(project(":libs:serverless-build-info"))
    compileOnly(project(":libs:serverless-shared-constants"))

    testImplementation("org.elasticsearch.test:framework")
    testImplementation("org.elasticsearch:server")
    testImplementation("org.elasticsearch:elasticsearch-cli")
    testImplementation("org.elasticsearch.test:s3-fixture")
}

var useS3Fixture = false

var s3Endpoint: String? = System.getenv("amazon_s3_endpoint")
var s3AccessKey: String? = System.getenv("amazon_s3_access_key")
var s3SecretKey: String? = System.getenv("amazon_s3_secret_key")
var s3Bucket: String? = System.getenv("amazon_s3_bucket")
var s3BasePath: String? = System.getenv("amazon_s3_base_path")

// s3Endpoint is optional, the rest control whether to use the fixture

if (s3AccessKey == null && s3SecretKey == null && s3Bucket == null && s3BasePath == null) {
    s3AccessKey = "s3_test_access_key"
    s3SecretKey = "s3_test_secret_key"
    s3Bucket = "bucket"
    s3BasePath = "base_path_integration_tests"

    useS3Fixture = true
} else if (s3AccessKey == null || s3SecretKey == null || s3Bucket == null || s3BasePath == null) {
    throw IllegalArgumentException("not all options specified to run against external S3 service are present")
}

val s3ThirdPartyTest by tasks.registering(Test::class) {
    outputs.doNotCacheIf("Build cache is disabled for Docker tests") { true }
    maxParallelForks = 1
    include("**/*IT.class")
    systemProperty("tests.security.manager", false)

    systemProperty("tests.use.fixture", useS3Fixture)
    systemProperty("test.s3.endpoint", s3Endpoint ?: "")
    systemProperty("test.s3.account", s3AccessKey ?: "")
    systemProperty("test.s3.key", s3SecretKey ?: "")
    systemProperty("test.s3.bucket", s3Bucket ?: "")
    systemProperty("test.s3.base", s3BasePath ?: "")

    val testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME)
    setClasspath(testSourceSet.getRuntimeClasspath())
    setTestClassesDirs(testSourceSet.getOutput().getClassesDirs())
}

tasks.named("check").configure {
    dependsOn(s3ThirdPartyTest)
}
