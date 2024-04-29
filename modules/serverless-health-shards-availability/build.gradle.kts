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
    id("elasticsearch.internal-es-plugin")
    id("elasticsearch.internal-java-rest-test")
    id("elasticsearch.internal-cluster-test")
    id("elasticsearch.internal-test-artifact")
}

esplugin {
    name = "serverless-health-shards-availability"
    description = "Overrides Shards Availability indicator with Serverless-specific version"
    classname = "co.elastic.elasticsearch.serverless.health.ServerlessShardsHealthPlugin"
}

configurations {
    all {
        resolutionStrategy {
            preferProjectModules()
        }
    }
}

dependencies {
    implementation("org.elasticsearch:server")
    internalClusterTestImplementation(testArtifact(project(":modules:stateless"), "internalClusterTest"))
}
