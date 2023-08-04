/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

plugins {
    id("elasticsearch.internal-yaml-rest-test")
}

dependencies {
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
    yamlRestTestRuntimeOnly(testArtifact(xpackModule("core")))
    yamlRestTestRuntimeOnly(testArtifact("org.elasticsearch.plugin.security.qa:service-account", "javaRestTest"))
}

restResources {
    restTests {
        includeXpack("*")
    }
}

tasks {
    yamlRestTest {
        usesDefaultDistribution()
        systemProperty("tests.rest.blacklist", listOf(
            // aggregate-metrics is sensitive to shards/replicas settings
            "aggregate-metrics/*/*",
            // data_streams uses searchable_snapshots
            "data_streams/*/*",
            // graph is sensitive to shards/replicas settings
            "graph/*/*",
            // health expects a data_content tier
            "health/*/*",
            // managing a license is not supported in serverless
            "license/*/*",
            // rollup has many tests failing with "Expected: <1> but: was <0>"
            "rollup/*/*",
            // searchable_snapshots doesn't exist in serverless
            "searchable_snapshots/*/*",
            // security/authz asserts on cat API output which has changed
            "security/authz/*/*",
            // snapshot has a problem with shards type null
            "snapshot/*/*",
            // spatial has a problem with serializing geo shape
            "spatial/*/*",
            // terms_enum has most tests failing with "field [terms] doesn't have length [1]"
            "terms_enum/*/*",
            // voting_only_node assumes data nodes
            "voting_only_node/*/*",
            // the license is fixed to "enterprise"
            "xpack/20_info/XPack Info API",
            // expects predictable node names
            "service_accounts/10_basic/Test service account tokens",
            // https://github.com/elastic/elasticsearch-serverless/issues/652
            "security/settings/10_update_security_settings/Test update and get security settings API").joinToString(","))
    }
}
