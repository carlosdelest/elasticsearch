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
    yamlRestTestImplementation(project(":modules:stateless"))
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
        systemProperty("tests.rest.blacklist", listOf(
            // aggregate-metrics is sensitive to shards/replicas settings
            "aggregate-metrics/*/*",
            // data_streams uses searchable_snapshots
            "data_streams/*/*",
            // esql is not available in serverless yet
            "esql/*/*",
            // health expects a data_content tier
            "health/*/*",
            // inference API isn't available in serverless yet...
            "inference/*/*",
            // this test asserts on ml node attributes that we don't reveal in serverless
            "ml/jobs_get_stats/Test get job stats after uploading data prompting the creation of some stats",
            // monitoring doesn't exist in serverless
            "monitoring/bulk/*/*",
            // rollup has many tests failing with "Expected: <1> but: was <0>"
            "rollup/*/*",
            // searchable_snapshots doesn't exist in serverless
            "searchable_snapshots/*/*",
            // snapshot has a problem with shards type null
            "snapshot/*/*",
            // voting_only_node assumes data nodes
            "voting_only_node/*/*",
            // monitoring and watcher are not supported in serverless, and the license is fixed to "enterprise"
            "xpack/*/*",
            // expects predictable node names
            "service_accounts/10_basic/Test service account tokens",
            // https://github.com/elastic/elasticsearch-serverless/issues/652
            "security/settings/10_update_security_settings/Test update and get security settings API",

            // Features not available on serverless (via API protections, etc)
            // managing a license is not supported in serverless
            "license/*/*",
            // Security users/roles
            "users/*/*",
            "change_password/*/*",
            "service_accounts/*/*",
            "roles/50_remote_only/*",
            // Uses "wait_for_active_shards"
            "security/authz/31_rollover_using_alias/*",
            // Some ML APIs
            "ml/post_data/*",
            // Deprecation
            "deprecation/*/*",
            // Downsample
            "security/authz/80_downsample/*",

            // Things that incidentally depend on endpoints that aren't available
            // - Deprecated ML deployment inference endpoint
            "ml/3rd_party_deployment/*",
            // - ML RestPostDataAction
            "ml/jobs_get_stats/*",
            "ml/ml_anomalies_default_mappings/*",
            "ml/job_cat_apis/*",

            // Learning to rank is disabled in serverless
            "ml/learning_to_rank_rescorer/*",

            // - CCS/CCR access keys
            "api_key/50_cross_cluster/*",
            // - Internal API using native user
            "api_key/12_grant/*",
            "privileges/40_get_user_privs/*",
            "security/authz/40_condtional_cluster_priv/*",
            // - Legacy templates
            "analytics/multi_terms/*",
            // Set of privileges is different in Serverless; we cover this case in
            // ServerlessCustomRolesIT::testGetBuiltinPrivileges
            "privileges/11_builtin/Test get builtin privileges",

            // Muted in severless, awaitsfix https://github.com/elastic/elasticsearch-serverless/issues/826
            "dlm/10_usage/Test data stream lifecycle usage stats",

            // Muted in severless, awaitsfix https://github.com/elastic/elasticsearch-serverless/issues/1833
            "ml/frequent_item_sets_agg/*",

            // Enrich stats API is not available on Serverless
            "enrich/10_basic/*",

            // awaitsFix https://github.com/elastic/elasticsearch-serverless/issues/1634
            "ml/p_value_significant_term_score/Test p_value significant terms score",

            // awaitsFix https://github.com/elastic/elasticsearch-serverless/issues/1777
            "ml/inference_crud/Test force delete given model with alias referenced by pipeline"
        ).joinToString(","))
    }
}

