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
    clusterModules(xpackModule("blob-cache"))
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
        systemProperty(
            "tests.rest.blacklist", listOf(

                // Those tests expect no relocations during execution and use 0 replicas for this,
                // they need adjustments (if possible) to work in Stateless
                "create/60_refresh/Refresh",
                "delete/50_refresh/Refresh",
                "index/60_refresh/Refresh",
                "update/60_refresh/Refresh",

                // Those tests expect refresh=wait_for to return a forced_refresh=false which is true in Stateless
                "create/60_refresh/refresh=wait_for waits until changes are visible in search",
                "delete/50_refresh/refresh=wait_for waits until changes are visible in search",
                "index/60_refresh/refresh=wait_for waits until changes are visible in search",
                "update/60_refresh/refresh=wait_for waits until changes are visible in search",

                // Those tests expect segment to be sorted on @timestamp field which only works on indexing shards
                // TODO ES-?
                "search/380_sort_segments_on_timestamp/Test if segments are missing @timestamp field we don't get errors",
                "search/380_sort_segments_on_timestamp/Test that index segments are NOT sorted on timestamp field when @timestamp field is dynamically added",
                "search/380_sort_segments_on_timestamp/Test that index segments are sorted on timestamp field if @timestamp field is defined in mapping",

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
                "indices.stats/60_field_usage/Field usage stats",

                // Those tests execute searches but expect a special number of shards,
                // they need adjustments (if possible) to work in Stateless
                "scroll/12_slices/Sliced scroll", //TODO ES-5354
                "scroll/12_slices/Sliced scroll with doc values", //TODO ES-5354
                "search/120_batch_reduce_size/batched_reduce_size 2 with 5 shards", // uses number_of_replicas: 0 and does not work with search shards
                "search/140_pre_filter_search_shards/pre_filter_shard_size with shards that have no hit", // require adjustments
                "search/160_exists_query/Test exists query *", // ? those tests failed more often
                "search_shards/*/*", //TODO ES-5354
                "search.vectors/50_dense_vector_field_usage/*", // deprecated API
                "tsdb/30_snapshot/Create a snapshot and then restore it", // waits for green status on 0 replicas but later executes search

                // Require Get API (and often DocWriteRequest ?refresh parameter support)
                "create/10_with_id/Create with ID",
                "exists/10_basic/Basic",
                "exists/70_defaults/Client-side default type",
                "index/10_with_id/Index with ID",
                "index/15_without_id/Index without ID",
                "index/30_cas/Compare And Swap Sequence Numbers",
                "index/91_metrics_no_subobjects/Metrics object indexing",
                "index/91_metrics_no_subobjects/Metrics object indexing with synthetic source",
                "index/91_metrics_no_subobjects/Root without subobjects",
                "index/91_metrics_no_subobjects/Root without subobjects with synthetic source",
                "indices.rollover/10_basic/Rollover index via API",
                "get/10_basic/Basic",
                "get/15_default_values/Default values",
                "get/20_stored_fields/Stored fields",
                "get/50_with_headers/REST test with headers",
                "get/70_source_filtering/Source filtering",
                "get/90_versions/Versions",
                "get/100_synthetic_source/doc values keyword with ignore_above",
                "get/100_synthetic_source/fetch without refresh also produces synthetic source",
                "get/100_synthetic_source/force_synthetic_source_bad_mapping",
                "get/100_synthetic_source/force_synthetic_source_ok",
                "get/100_synthetic_source/ip with ignore_malformed",
                "get/100_synthetic_source/indexed dense vectors",
                "get/100_synthetic_source/keyword",
                "get/100_synthetic_source/non-indexed dense vectors",
                "get/100_synthetic_source/stored keyword",
                "get/100_synthetic_source/stored keyword with ignore_above",
                "get/100_synthetic_source/stored text",
                "get/100_synthetic_source/_source filtering",
                "get/110_ignore_malformed/ip",
                "get_source/10_basic/Basic",
                "get_source/15_default_values/Default values",
                "get_source/70_source_filtering/Source filtering",
                "mget/10_basic/Basic multi-get",
                "mget/15_ids/IDs",
                "mget/12_non_existent_index/Non-existent index",
                "mget/13_missing_metadata/Missing metadata",
                "mget/14_alias_to_multiple_indices/Multi Get with alias that resolves to multiple indices",
                "mget/17_default_index/Default index/type",
                "mget/70_source_filtering/Source filtering -  exclude field",
                "mget/70_source_filtering/Source filtering -  ids and exclude field",
                "mget/70_source_filtering/Source filtering -  ids and include field",
                "mget/70_source_filtering/Source filtering -  ids and include nested field",
                "mget/70_source_filtering/Source filtering -  ids and true/false",
                "mget/70_source_filtering/Source filtering -  include field",
                "mget/70_source_filtering/Source filtering -  include nested field",
                "mget/70_source_filtering/Source filtering -  true/false",
                "mget/90_synthetic_source/force_synthetic_source_ok",
                "mget/90_synthetic_source/force_synthetic_source_bad_mapping",
                "mget/90_synthetic_source/keyword",
                "mget/90_synthetic_source/stored text",
                "update/10_doc/Partial document",
                "update/13_legacy_doc/Partial document",
                "update/20_doc_upsert/Doc upsert",
                "update/22_doc_as_upsert/Doc as upsert",
                "update/35_if_seq_no/Update with if_seq_no",
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

                // Require Get API and Search API as well as ES-5354
                "termvectors/10_basic/Basic tests for termvector get",
                "mtermvectors/10_basic/Basic tests for multi termvector get",

                // Probably unsupported in stateless
                "indices.shrink/*/*",
                "indices.split/*/*",
                "tsdb/80_index_resize/clone",
                "tsdb/80_index_resize/clone no source index",
                "tsdb/80_index_resize/shrink",
                "tsdb/80_index_resize/split",

            ).joinToString(",")
        )
    }
}
