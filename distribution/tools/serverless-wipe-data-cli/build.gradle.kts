import org.elasticsearch.gradle.VersionProperties
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

val versions = VersionProperties.getVersions()
val nettyVer = versions["netty"]

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

    // Azure sdk
    api("com.azure:azure-core:1.53.0")
    api("com.azure:azure-storage-common:12.27.1")
    api("com.azure:azure-storage-blob:12.28.1")
    api("com.azure:azure-storage-blob-batch:12.24.0")
    api("com.azure:azure-json:1.3.0")
    api("com.azure:azure-xml:1.1.0")
    api("com.azure:azure-core-http-netty:1.15.5")

    api("io.netty:netty-buffer:${nettyVer}")
    api("io.netty:netty-codec:${nettyVer}")
    api("io.netty:netty-codec-http:${nettyVer}")
    api("io.netty:netty-codec-http2:${nettyVer}")
    api("io.netty:netty-common:${nettyVer}")
    api("io.netty:netty-handler:${nettyVer}")
    api("io.netty:netty-handler-proxy:${nettyVer}")
    api("io.netty:netty-resolver:${nettyVer}")
    api("io.netty:netty-resolver-dns:${nettyVer}")
    api("io.netty:netty-transport:${nettyVer}")
    api("io.netty:netty-transport-native-unix-common:${nettyVer}")
    api("io.projectreactor:reactor-core:3.4.38")
    api("io.projectreactor.netty:reactor-netty-http:1.1.22")
    api("io.projectreactor.netty:reactor-netty-core:1.1.22")
    api("io.projectreactor.netty:reactor-netty:1.1.22")
    api("org.slf4j:slf4j-api:${versions["slf4j"]}")
    api("org.slf4j:slf4j-simple:${versions["slf4j"]}")
    api("org.reactivestreams:reactive-streams:1.0.4")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.12.7")

    // com.amazonaws.util.Base64 complains about performance if you don't have jaxb available
    api("javax.xml.bind:jaxb-api:2.2.2")

    compileOnly("org.elasticsearch:server")
    compileOnly("org.elasticsearch:cli")
    compileOnly("org.elasticsearch:server-cli")

    // serverless libs are placed on the server classpath, so they are already present at runtime
    compileOnly(project(":libs:serverless-build-info"))
    compileOnly(project(":libs:serverless-shared-constants"))

    testImplementation("org.elasticsearch.test:framework")
    testImplementation("org.elasticsearch:server")
    testImplementation("org.elasticsearch:cli")
    testImplementation("org.elasticsearch.test:s3-fixture")
    testImplementation("org.elasticsearch.test:azure-fixture")
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
    include("**/s3/*IT.class")
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


var useAzureFixture = false

var azureEndpoint: String? = System.getenv("azure_blob_endpoint")
var azureSasToken: String? = System.getenv("azure_blob_sas_token")
var azureContainer: String? = System.getenv("azure_blob_container")
var azureAccount: String? = System.getenv("azure_blob_account")
var azureAccountKey: String? = System.getenv("azure_blob_account_key")

if (azureEndpoint == null && azureContainer == null && azureSasToken == null && azureAccount == null && azureAccountKey == null) {
    // default using fixture
    useAzureFixture = true
    azureEndpoint = "test_endpoint"
    azureContainer = "test_container"
    azureSasToken = "test_sas_token"
    // account key is being validated for format, just use the default credential for Azurite:
    // https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage#well-known-storage-account-and-key
    azureAccount = "devstoreaccount1"
    azureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
} else if (azureEndpoint == null || azureContainer == null) {
    // required if not using fixture
    throw IllegalArgumentException("azure_blob_endpoint or azure_blob_container must be present for external Azure blob service")
} else if (azureSasToken == null && (azureAccount == null || azureAccountKey == null)) {
    // if not using fixture, authorize with:
    //  1) sas token, or
    //  2) account and account key
    throw IllegalArgumentException("not all options specified to run against external Azure blob service are present")
}

val azureThirdPartyTest by tasks.registering(Test::class) {
    outputs.doNotCacheIf("Build cache is disabled for Docker tests") { true }
    maxParallelForks = 1
    include("**/azure/*IT.class")
    systemProperty("tests.security.manager", false)

    systemProperty("test.azure.use.fixture", useAzureFixture)
    systemProperty("test.azure.endpoint", azureEndpoint ?: "")
    systemProperty("test.azure.container", azureContainer ?: "")
    systemProperty("test.azure.sas_token", azureSasToken ?: "")
    systemProperty("test.azure.account", azureAccount ?: "")
    systemProperty("test.azure.account_key", azureAccountKey ?: "")

    val testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME)
    setClasspath(testSourceSet.getRuntimeClasspath())
    setTestClassesDirs(testSourceSet.getOutput().getClassesDirs())
}

tasks.named("check").configure {
    dependsOn(s3ThirdPartyTest)
    dependsOn(azureThirdPartyTest)
}
