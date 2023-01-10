import org.elasticsearch.gradle.internal.test.rest.CopyRestApiTask

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
    id("elasticsearch.internal-yaml-rest-test")
}

dependencies {
    clusterModules("org.elasticsearch.plugin:mapper-extras")
}

restResources {
    restTests {
        includeCore("*")
    }
}

tasks.withType(CopyRestApiTask::class) {
    // This project doesn't have any tests of its own. It's just running the core elasticsearch rest tests.
    isSkipHasRestTestCheck = true
}

tasks.named("yamlRestTest", Test::class) {
    systemProperty(
        "tests.rest.blacklist", listOf(
            "health/40_diagnosis/Diagnosis",
            "cat.nodes/10_basic/Test cat nodes output",
            "cluster.stats/10_basic/cluster stats test",
            "cluster.stats/10_basic/get cluster stats returns cluster_uuid at the top level",
            "cluster.desired_balance/10_basic/Test cluster_balance_stats" //This test expects different data tiers as one provided by stateless
        ).joinToString(",")
    )
}
