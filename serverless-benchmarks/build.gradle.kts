import org.elasticsearch.gradle.VersionProperties

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
    id("elasticsearch.java-base")
    id("java-library")
    id("application")
}

val versions = VersionProperties.getVersions()

dependencies {
    api("org.elasticsearch:server") {
        exclude(group = "net.sf.jopt-simple", module = "jopt-simple")
    }
    api("org.openjdk.jmh:jmh-core:${versions["jmh"]}")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:${versions["jmh"]}")
    // Dependencies of JMH
    runtimeOnly("net.sf.jopt-simple:jopt-simple:5.0.4")
    runtimeOnly("org.apache.commons:commons-math3:3.6.1")
}

application {
    mainClass = "org.openjdk.jmh.Main"
}

tasks {
    assemble {
        enabled = false
    }
    test {
        enabled = false
    }
    javadoc {
        enabled = false
    }
    compileJava {
        options.compilerArgs.addAll(listOf("-processor", "org.openjdk.jmh.generators.BenchmarkProcessor"))
        // org.elasticsearch.plugins.internal is used in signatures classes used in benchmarks but we don't want to expose it publicly
        // adding an export to allow compilation with gradle. This does not solve a problem in intellij as it does not use compileJava task
        options.compilerArgs.addAll(listOf("--add-exports", "org.elasticsearch.server/org.elasticsearch.plugins.internal=ALL-UNNAMED"))
    }
    val run by named<JavaExec>("run") {
        executable = "${buildParams.runtimeJavaHome.get()}/bin/java"
    }
}
