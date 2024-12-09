/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.gradle.internal.test.RestIntegTestTask
import org.elasticsearch.gradle.internal.test.rest.InternalYamlRestTestPlugin

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
    val blacklist = listOf(
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
        // rollup doesn't exist in serverless
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
        // Uses data stream stats
        "security/authz/50_data_streams/Test that data streams stats is limited to authorized namespace",

        // Things that incidentally depend on endpoints that aren't available
        // - Deprecated ML deployment inference endpoint
        "ml/3rd_party_deployment/*",
        // - ML RestPostDataAction
        "ml/jobs_get_stats/*",
        "ml/ml_anomalies_default_mappings/*",
        "ml/job_cat_apis/*",

        // - CCS/CCR access keys
        "api_key/50_cross_cluster/*",
        // - Internal API using native user
        "api_key/12_grant/*",
        "privileges/40_get_user_privs/*",
        "security/authz/40_condtional_cluster_priv/*",
        // - Invalid application privileges assigned to roles
        "privileges/20_has_application_privs/*",

        // - Legacy templates
        "analytics/multi_terms/*",
        // Set of privileges is different in Serverless; we cover this case in
        // ServerlessCustomRolesIT::testGetBuiltinPrivileges
        "privileges/11_builtin/Test get builtin privileges",

        // Enrich stats API is not available on Serverless
        "enrich/10_basic/*",

        // awaitsFix https://github.com/elastic/elasticsearch-serverless/issues/1634
        "ml/p_value_significant_term_score/Test p_value significant terms score",

        // awaitsFix https://github.com/elastic/elasticsearch-serverless/issues/1777
        "ml/inference_crud/Test force delete given model with alias referenced by pipeline",

        // Bulk roles is not available in serverless
        "roles/60_bulk_roles/*",
        // Global privileges not supported in Serverless
        "roles/40_global_privileges/*",
        // The migrate functionality will not be used in serverless yet
        "migrate/*/*"
    )
    yamlRestTest {
        systemProperty("tests.rest.blacklist", blacklist.joinToString(","))
    }

    val yamlRestTestWithMultiProject = register<RestIntegTestTask>("yamlRestTestWithMultiProject") {
        val sourceSet = sourceSets.getByName(InternalYamlRestTestPlugin.SOURCE_SET_NAME)
        setTestClassesDirs(sourceSet.getOutput().getClassesDirs())
        setClasspath(sourceSet.getRuntimeClasspath())

        val blacklistMultiProject = mutableListOf(
            // The following do not work in a multi-project setup with stateless
            "^serverless/autoscaling/*/*",
            "security/authz/13_index_datemath/*", // Fails because shards did come online in time, but it seems consistent

            /* These tests don't work on multi-project yet - we need to go through each of them and make them work */
            "^analytics/boxplot/*",
            "^analytics/histogram/*",
            "^analytics/moving_percentiles/*",
            "^analytics/top_metrics/*",
            "^analytics/usage/*",
            "^constant_keyword/10_basic/*",
            "^data_streams/10_data_stream_resolvability/*",
            "^deprecation/10_basic/*",
            "^dlm/10_usage/*",
            "^enrich/10_basic/*",
            "^enrich/20_standard_index/*",
            "^enrich/30_tsdb_index/*",
            "^enrich/40_synthetic_source/*",
            "^enrich/50_data_stream/*",
            "^esql/40_tsdb/*",
            "^esql/45_non_tsdb_counter/*",
            "^esql/60_enrich/*",
            "^esql/60_usage/*",
            "^esql/61_enrich_ip/*",
            "^esql/62_extra_enrich/*",
            "^esql/63_enrich_int_range/*",
            "^esql/64_enrich_int_match/*",
            "^health/10_usage/*",
            "^ilm/10_basic/Test Undeletable Policy In Use",
            "^ilm/20_move_to_step/*",
            "^ilm/30_retry/*",
            "^ilm/40_explain_lifecycle/*",
            "^ilm/60_operation_mode/*",
            "^ilm/60_remove_policy_for_index/*",
            "^ilm/70_downsampling/*",
            "^ilm/80_health/*",
            "^logsdb/10_usage/*",
            "^ml/3rd_party_deployment/*",
            "^ml/bucket_correlation_agg/*",
            "^ml/bucket_count_ks_test_agg/*",
            "^ml/calendar_crud/*",
            "^ml/categorization_agg/*",
            "^ml/change_point_agg/*",
            "^ml/custom_all_field/*",
            "^ml/data_frame_analytics_cat_apis/*",
            "^ml/data_frame_analytics_crud/*",
            "^ml/datafeed_cat_apis/*",
            "^ml/datafeeds_crud/*",
            "^ml/delete_expired_data/*",
            "^ml/delete_job_force/*",
            "^ml/explain_data_frame_analytics/*",
            "^ml/filter_crud/*",
            "^ml/forecast/*",
            "^ml/frequent_item_sets_agg/*",
            "^ml/get_datafeed_stats/*",
            "^ml/get_datafeeds/*",
            "^ml/get_memory_stats/*",
            "^ml/get_model_snapshots/*",
            "^ml/get_model_snapshots/*/*",
            "^ml/get_trained_model_stats/*",
            "^ml/inference_crud/*",
            "^ml/inference_processor/*",
            "^ml/job_cat_apis/*",
            "^ml/job_groups/*",
            "^ml/jobs_crud/*",
            "^ml/jobs_get/*",
            "^ml/jobs_get_result_buckets/*",
            "^ml/jobs_get_result_categories/*",
            "^ml/jobs_get_result_influencers/*",
            "^ml/jobs_get_result_overall_buckets/*",
            "^ml/jobs_get_result_records/*",
            "^ml/jobs_get_stats/*",
            "^ml/learning_to_rank_rescorer/*",
            "^ml/ml_anomalies_default_mappings/*",
            "^ml/ml_info/*",
            "^ml/p_value_significant_term_score/*",
            "^ml/pipeline_inference/*",
            "^ml/post_data/*",
            "^ml/preview_data_frame_analytics/*",
            "^ml/preview_datafeed/*",
            "^ml/reset_job/*",
            "^ml/revert_model_snapshot/*",
            "^ml/search_knn_query_vector_builder/*",
            "^ml/set_upgrade_mode/*",
            "^ml/sparse_vector_search/*",
            "^ml/start_data_frame_analytics/*",
            "^ml/start_stop_datafeed/*",
            "^ml/stop_data_frame_analytics/*",
            "^ml/stop_data_frame_analytics/Test stop with inconsistent body/param ids",
            "^ml/text_embedding_search/*",
            "^ml/text_expansion_search/*",
            "^ml/text_expansion_search_rank_features/*",
            "^ml/text_expansion_search_sparse_vector/*",
            "^ml/trained_model_cat_apis/*",
            "^ml/update_trained_model_deployment/*",
            "^ml/upgrade_job_snapshot/*",
            "^monitoring/bulk/10_basic/*",
            "^monitoring/bulk/20_privileges/*",
            "^profiling/10_basic/*",
            "^redact/10_redact_processor/*",
            "^role_mapping/10_basic/*",
            "^role_mapping/20_get_missing/*",
            "^role_mapping/30_delete/*",
            "^rollup/delete_job/*",
            "^rollup/get_jobs/*",
            "^rollup/get_rollup_caps/*",
            "^rollup/get_rollup_index_caps/*",
            "^rollup/put_job/*",
            "^rollup/rollup_search/*",
            "^rollup/start_job/*",
            "^rollup/stop_job/*",
            "^searchable_snapshots/10_usage/*",
            "^searchable_snapshots/20_synthetic_source/*",
            "^security/authz/12_index_alias/*",
            "^security/authz/14_cat_indices/*",
            "^security/authz/14_cat_indices/Test explicit request while multiple opened/*",
            "^security/authz/30_dynamic_put_mapping/*",
            "^security/authz/31_rollover_using_alias/*",
            "^security/authz/50_data_streams/*",
            "^security/authz/51_data_stream_aliases/*",
            "^security/authz/60_resolve_index/*",
            "^security/authz/80_downsample/*",
            "^security/settings/10_update_security_settings/*",
            "^set_security_user/10_small_users_one_index/*",
            "^set_security_user/20_api_key/*",
            "^snapshot/10_basic/*",
            "^snapshot/20_operator_privileges_disabled/*",
            "^spatial/50_feature_usage/*",
            "^spatial/100_geo_grid_ingest/*",
            "^transform/preview_transforms/*",
            "^transform/transforms_cat_apis/*",
            "^transform/transforms_crud/*",
            "^transform/transforms_force_delete/*",
            "^transform/transforms_reset/*",
            "^transform/transforms_start_stop/*",
            "^transform/transforms_start_stop/Test start/stop only starts/stops specified transform",
            "^transform/transforms_start_stop/Test start/stop with field alias",
            "^transform/transforms_start_stop/Test start/stop/start continuous transform",
            "^transform/transforms_start_stop/Test start/stop/start transform",
            "^transform/transforms_stats/*",
            "^transform/transforms_stats_continuous/*",
            "^transform/transforms_unattended/*",
            "^transform/transforms_update/*",
            "^transform/transforms_upgrade/*",
            "^voting_only_node/10_basic/*"
        )

        blacklistMultiProject.addAll(blacklist)
        systemProperty("tests.rest.blacklist", blacklistMultiProject.joinToString(","))
        systemProperty("es.test.multi_project.enabled", "true")
    }

    check {
        dependsOn(yamlRestTestWithMultiProject)
    }
}

