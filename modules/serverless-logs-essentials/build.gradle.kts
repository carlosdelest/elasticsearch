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
    name = "serverless-logs-essentials"
    description = "Serverless module for observability logs essentials tier"
    classname = "co.elastic.elasticsearch.serverless.observability.LogsEssentialsPlugin"
    extendedPlugins = listOf("x-pack-esql")
}

dependencies {
    compileOnly(project(":libs:serverless-shared-constants"))
    compileOnly(xpackModule("esql"))
    compileOnly(xpackModule("esql-core"))

    javaRestTestImplementation(project(":libs:serverless-shared-constants"))
}

tasks {
    javaRestTest {
        usesDefaultDistribution("depends on variety of other serverless and stateful modules")
    }
}
