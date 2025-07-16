import org.elasticsearch.gradle.VersionProperties

plugins {
    id("elasticsearch.java")
    id("elasticsearch.cache-test-fixtures")
}

description = "UIAM Service Test Fixture"

val versions = VersionProperties.getVersions()

repositories {
    mavenCentral()
}

dependencies {
    api("org.testcontainers:testcontainers:${versions["testcontainer"]}")
    api("com.github.docker-java:docker-java-api:${versions["dockerJava"]}")
    api("org.elasticsearch.test:testcontainer-utils")
    api("junit:junit:${versions["junit"]}")
    implementation("org.slf4j:slf4j-api:${versions["slf4j"]}")

    runtimeOnly("com.github.docker-java:docker-java-transport-zerodep:${versions["dockerJava"]}")
    runtimeOnly("com.github.docker-java:docker-java-transport:${versions["dockerJava"]}")
    runtimeOnly("com.github.docker-java:docker-java-core:${versions["dockerJava"]}")
    runtimeOnly("org.apache.commons:commons-compress:${versions["commonsCompress"]}")
    runtimeOnly("org.rnorth.duct-tape:duct-tape:${versions["ductTape"]}")
    runtimeOnly("com.fasterxml.jackson.core:jackson-core:${versions["jackson"]}")
    runtimeOnly("com.fasterxml.jackson.core:jackson-annotations:${versions["jackson"]}")
}
