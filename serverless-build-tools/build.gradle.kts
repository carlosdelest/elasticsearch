plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    api("org.elasticsearch:build-conventions")
    api("org.elasticsearch.gradle:build-tools")
    api("org.elasticsearch.gradle:build-tools-internal")
    implementation("org.jetbrains.kotlin:kotlin-script-runtime")
}
