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

dependencies {
    javaRestTestImplementation(testArtifact(xpackModule("plugin")))
    javaRestTestImplementation(testArtifact(xpackModule("core")))
    javaRestTestImplementation(testArtifact("org.elasticsearch.plugin.security.qa:service-account", "javaRestTest"))
}

tasks {
    javaRestTest { ->
        onlyIf("E2E test task must be invoked directly", { directlyInvoked(this) })
        doFirst {
            println("Running E2E tests with ESS_PUBLIC_URL = ${System.getenv().get("ESS_PUBLIC_URL")}")
            println("Running E2E tests with ESS_PROJECT_ID = ${System.getenv().get("ESS_PROJECT_ID")}")
            println("Running E2E tests with ESS_API_KEY_ENCODED = ${System.getenv().get("ESS_API_KEY_ENCODED")}")
        }
    }
}

fun Build_gradle.directlyInvoked(task: Task) = gradle.startParameter.getTaskNames().contains(task.getPath()) ||
        (gradle.startParameter.getTaskNames().contains(task.getName()) && gradle.startParameter.currentDir == project.projectDir)
