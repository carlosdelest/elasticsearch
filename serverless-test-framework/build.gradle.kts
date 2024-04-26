import org.elasticsearch.gradle.VersionProperties

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

plugins {
    id("elasticsearch.java")
}

val versions = VersionProperties.getVersions()

dependencies {
    implementation("org.elasticsearch.test:test-clusters")
    implementation("org.apache.logging.log4j:log4j-api:${versions["log4j"]}")
    implementation("org.apache.commons:commons-lang3:${versions["commons_lang3"]}")
    implementation(project(":libs:serverless-build-info"))
    implementation("org.elasticsearch.test:framework")
}
