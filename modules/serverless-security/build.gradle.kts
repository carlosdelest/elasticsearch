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
    id("elasticsearch.internal-java-rest-test")
}

esplugin {
    name = "serverless-security"
    description = "Serverless security module for Elasticsearch"
    classname = "co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin"
    extendedPlugins = listOf("x-pack-security")
}

dependencies {
    javaRestTestImplementation(project(":modules:serverless-security"))
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    javaRestTestImplementation(testArtifact(xpackModule("core")))
    testImplementation(testArtifact(xpackModule("core")))
    compileOnly(xpackModule("core"))
    compileOnly(xpackModule("security"))
}
