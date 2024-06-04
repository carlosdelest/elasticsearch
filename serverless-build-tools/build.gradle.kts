plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
}

repositories {
    mavenCentral()
    // for the custom shadow plugin version we currently use for 8.8 compatibility
    maven {
        name = "jitpack"
        url = uri("https://jitpack.io")
    }
    gradlePluginPortal()
}

dependencies {
    api("org.elasticsearch:build-conventions")
    api("org.elasticsearch.gradle:build-tools")
    api("org.elasticsearch.gradle:build-tools-internal")
    implementation("org.jetbrains.kotlin:kotlin-script-runtime")
}

gradlePlugin {
    isAutomatedPublishing = false
    plugins {
        val serverlessDistributionDownload by creating {
            id = "elasticsearch.serverless-distribution-download"
            implementationClass = "org.elasticsearch.gradle.serverless.ServerlessDistributionDownloadPlugin"
        }
    }
}
