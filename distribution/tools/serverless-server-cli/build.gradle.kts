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
    compileOnly("org.elasticsearch:server")
    compileOnly("org.elasticsearch:elasticsearch-cli")
    compileOnly("org.elasticsearch:server-cli")

    // serverless libs are placed on the server classpath, so they are already present at runtime
    compileOnly(project(":libs:serverless-build-info"))
    compileOnly(project(":libs:serverless-shared-constants"))

    testImplementation("org.elasticsearch.test:framework")
    testImplementation("org.elasticsearch:server")
    testImplementation("org.elasticsearch:elasticsearch-cli")
}

tasks {
  test {
    systemProperty("tests.system_call_filter", "false")
  }
}
