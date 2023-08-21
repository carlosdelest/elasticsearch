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
    yamlRestTestImplementation(testArtifact(xpackModule("plugin")))
}

restResources {
    restTests {
        includeCore("*")
    }
}

tasks {
    copyRestApiSpecsTask {
        // This project doesn't have any tests of its own. It's just running the core elasticsearch rest tests.
        isSkipHasRestTestCheck = true
    }
    yamlRestTest {
        usesDefaultDistribution()
        systemProperty(
            "tests.rest.blacklist", listOf(

                // Those tests expect no relocations during execution and use 0 replicas for this,
                // they need adjustments (if possible) to work in Stateless
                "create/60_refresh/Refresh",
                "delete/50_refresh/Refresh",
                "index/60_refresh/Refresh",
                "update/60_refresh/Refresh",

                //this test is failing with security enabled
                //https://github.com/elastic/elasticsearch/issues/32238
                // and https://github.com/elastic/elasticsearch/issues/74540
                "indices.get_alias/10_basic/Get alias against closed indices",
                // Those tests compute stats from any shards and that don't play well with search shards
                "indices.stats/13_fields/Completion fields - multi",
                "indices.stats/13_fields/Completion fields - one",
                "indices.stats/13_fields/Completion fields - star",
                "indices.stats/13_fields/Completion - all metric",
                "indices.stats/13_fields/Completion - multi metric",
                "indices.stats/13_fields/Completion - one metric",
                "indices.stats/13_fields/Completion - pattern",
                "indices.stats/13_fields/Fielddata fields - all metric",
                "indices.stats/13_fields/Fielddata fields - multi",
                "indices.stats/13_fields/Fielddata fields - multi metric",
                "indices.stats/13_fields/Fielddata fields - one",
                "indices.stats/13_fields/Fielddata fields - one metric",
                "indices.stats/13_fields/Fielddata fields - pattern",
                "indices.stats/13_fields/Fielddata fields - star",
                "indices.stats/13_fields/Fields - blank",
                "indices.stats/13_fields/Fields - completion metric",
                "indices.stats/13_fields/Fields - fielddata metric",
                "indices.stats/13_fields/Fields - one",
                "indices.stats/13_fields/Fields - pattern",
                "indices.stats/13_fields/Fields - multi",
                "indices.stats/13_fields/Fields - multi metric",
                "indices.stats/13_fields/Fields - star",
                "indices.stats/13_fields/Fields - _all metric",
                // The following were added in https://github.com/elastic/elasticsearch/pull/94500
                // (and moved to 90_global_ordinals later)
                "indices.stats/90_global_ordinals/*",
                "indices.stats/60_field_usage/Field usage stats",

                // Temporarily Muted due to refresh_interval change
                "indices.put_settings/all_path_options/put settings in prefix* index",
                "indices.put_settings/all_path_options/put settings per index",
                "indices.get_settings/30_defaults/Test retrieval of default settings",

                // Those tests execute searches but expect a special number of shards,
                // they need adjustments (if possible) to work in Stateless
                "search/120_batch_reduce_size/batched_reduce_size 2 with 5 shards", // uses number_of_replicas: 0 and does not work with search shards
                "search/140_pre_filter_search_shards/pre_filter_shard_size with shards that have no hit", // require adjustments
                "search/160_exists_query/Test exists query *", // ? those tests failed more often
                "search.vectors/50_dense_vector_field_usage/*", // deprecated API
                "tsdb/30_snapshot/Create a snapshot and then restore it", // waits for green status on 0 replicas but later executes search

                // Require the Disk Usage API
                "mget/90_synthetic_source/keyword",
                "mget/90_synthetic_source/stored text",
                "update/100_synthetic_source/stored text",
                "update/100_synthetic_source/keyword",

                // Require Data Tiers
                "health/40_diagnosis/Diagnosis", // expects data tier data_content and returns 2 indicators.shards_availability.diagnosis
                "cluster.desired_balance/10_basic/Test cluster_balance_stats", // This test expects different data tiers as one provided by stateless

                // The following two tests make assertions on the number of nodes with the "data" role, which is
                // not a valid role in stateless.
                // TODO: We would probably require a similar test to assert node counts for "index" and "search" roles.
                "cluster.stats/10_basic/cluster stats test",
                "cluster.stats/10_basic/get cluster stats returns cluster_uuid at the top level",

                // Require the Node Stats API with index level metrics to correctly count mappings
                "nodes.stats/11_indices_metrics/indices mappings does not exist in shards level", // Failure at [nodes.stats/11_indices_metrics:540]: field [nodes.$node_id.indices.mappings.total_count] is not greater than or equal to [2]
                "nodes.stats/11_indices_metrics/indices mappings exact count test for indices level", // Failure at [nodes.stats/11_indices_metrics:502]: field [nodes.$node_id.indices.mappings.total_count] is not greater than or equal to [26]

                // Probably unsupported in stateless
                "indices.shrink/*/*",
                "indices.split/*/*",
                "tsdb/80_index_resize/clone",
                "tsdb/80_index_resize/clone no source index",
                "tsdb/80_index_resize/shrink",
                "tsdb/80_index_resize/split",

                // AssertionError: Failure at [indices.stats/50_disk_usage:50]: value of [testindex.store_size_in_bytes] is not comparable (got [null])
                // see https://gradle-enterprise.elastic.co/s/ezssvnid7qjnm/console-log?task=:qa:core-rest-test:yamlRestTest
                "tsdb/110_field_caps/field caps on time_series indices",
                "field_caps/40_time_series/Get simple time series field caps",
                "indices.validate_query/10_basic/Validate query api",
                "tsdb/110_field_caps/field caps on standard indices",
                "indices.stats/50_disk_usage/Dense vectors",
                "field_caps/40_time_series/Get time series field caps with conflicts",
                "indices.stats/50_disk_usage/Name the index",
                "tsdb/110_field_caps/field caps on mixed indices",
                "indices.stats/50_disk_usage/Star",

                // ignored untill we can recover a real primary shard
                "indices.stats/30_segments/Segment Stats",
                "indices.clone/10_basic/Clone index via API",

                // AwaitsFix: https://github.com/elastic/elasticsearch-serverless/issues/541
                "synonyms/90_synonyms_reloading_for_synset/Reload analyzers for specific synonym set",

                // Adjusting the voting configuration is not possible in stateless
                "cluster.voting_config_exclusions/*/*"
            ).joinToString(",")
        )
    }
}
