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

tasks {
    copyRestApiSpecsTask {
        // This project doesn't have any tests of its own. It's just running the core elasticsearch rest tests.
        isSkipHasRestTestCheck = true
    }
    yamlRestTest {
        systemProperty(
            "tests.rest.blacklist", listOf(

                // Require "search" shards and BulkRequest ?refresh parameter support
                "bulk/10_basic/Array of objects",
                "bulk/10_basic/Empty _id",
                "bulk/10_basic/Empty _id with op_type create",
                "bulk/11_dynamic_templates/Dynamic templates",
                "bulk/11_dynamic_templates/Dynamic templates with op_type",
                "bulk/20_list_of_strings/List of strings",
                "bulk/30_big_string/One big string",
                "bulk/50_refresh/refresh=empty string immediately makes changes are visible in search",
                "bulk/50_refresh/refresh=true immediately makes changes are visible in search",
                "bulk/50_refresh/refresh=wait_for waits until changes are visible in search",
                "bulk/90_pipeline/One request has pipeline and another not",
                "index/90_geo_point/null value in array",
                "index/90_geo_point/single null value",
                "tsdb/10_settings/check end_time boundary with data_nano",
                "tsdb/10_settings/check start_time and end_time with data_nano",
                "tsdb/10_settings/check start_time boundary with data_nano",
                "tsdb/10_settings/set start_time and end_time",
                "tsdb/15_timestamp_mapping/date",
                "tsdb/15_timestamp_mapping/date_nanos",
                "tsdb/15_timestamp_mapping/automatically add with date",
                "tsdb/25_id_generation/ids query",
                "tsdb/25_id_generation/index a new document on top of an old one",
                "tsdb/25_id_generation/index a new document on top of an old one over bulk",
                "tsdb/30_snapshot/Create a snapshot and then restore it", // also require Close API but can be changed to delete
                "tsdb/50_alias/search an alias",
                "tsdb/50_alias/index into alias",
                "tsdb/40_search/aggregate a dimension",
                "tsdb/40_search/aggregate a metric",
                "tsdb/40_search/aggregate a tag",
                "tsdb/40_search/aggregate the tsid",
                "tsdb/40_search/fetch a dimension",
                "tsdb/40_search/fetch a metric",
                "tsdb/40_search/fetch a tag",
                "tsdb/40_search/fetch the tsid",
                "tsdb/40_search/query a dimension",
                "tsdb/40_search/query a metric",
                "tsdb/40_search/sort by tsid",
                "tsdb/60_add_dimensions/add dimensions to no dims with dynamic_template over bulk",
                "tsdb/60_add_dimensions/add dimensions to some dims with dynamic_template over bulk",
                "tsdb/70_dimension_types/ip dimension",
                "tsdb/70_dimension_types/keyword dimension",
                "tsdb/70_dimension_types/long dimension",
                "tsdb/90_unsupported_operations/aggregate by _id",
                "tsdb/90_unsupported_operations/noop update",
                "tsdb/90_unsupported_operations/search with routing",
                "tsdb/90_unsupported_operations/sort by _id",
                "tsdb/100_composite/composite aggregation on tsid",
                "tsdb/100_composite/composite aggregation on tsid with after",
                "tsdb/90_unsupported_operations/aggregate on _id",

                // Require "search" shards and DocWriteRequest ?refresh parameter support
                "cat.count/10_basic/Test cat count output",
                "create/60_refresh/refresh=wait_for waits until changes are visible in search",
                "create/60_refresh/When refresh url parameter is an empty string that means \"refresh immediately\"",
                "delete/50_refresh/Refresh",
                "delete/50_refresh/refresh=wait_for waits until changes are visible in search",
                "delete/50_refresh/When refresh url parameter is an empty string that means \"refresh immediately\"",
                "index/60_refresh/Refresh",
                "index/60_refresh/refresh=wait_for waits until changes are visible in search",
                "index/60_refresh/When refresh url parameter is an empty string that means \"refresh immediately\"",
                "tsdb/15_timestamp_mapping/explicitly enable timestamp meta field",
                "tsdb/60_add_dimensions/add dimensions with put_mapping",
                "tsdb/60_add_dimensions/add dimensions to no dims with dynamic_template over index",
                "tsdb/60_add_dimensions/add dimensions to some dims with dynamic_template over index",
                "update/60_refresh/Refresh",
                "update/60_refresh/refresh=wait_for waits until changes are visible in search",
                "update/60_refresh/When refresh url parameter is an empty string that means \"refresh immediately\"",

                // Require "search" shards
                "cat.fielddata/10_basic/Test cat fielddata output",
                "create/60_refresh/Refresh",
                "count/10_basic/count with body",
                "count/10_basic/count with empty body",
                "count/20_query_string/count with query_string parameters",
                "explain/10_basic/Basic explain",
                "explain/10_basic/Basic explain with alias",
                "explain/20_source_filtering/Source filtering",
                "explain/30_query_string/explain with query_string parameters",
                "field_caps/30_index_filter/Field caps with index filter",
                "indices.blocks/10_basic/Basic test for index blocks",
                "indices.open/10_basic/Basic test for index open/close",
                "indices.open/20_multiple_indices/All indices",
                "indices.open/20_multiple_indices/Only wildcard",
                "indices.open/20_multiple_indices/Trailing wildcard",
                "indices.sort/10_basic/Index Sort",
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
                "mlt/10_basic/Basic mlt",
                "mlt/20_docs/Basic mlt query with docs",
                "mlt/30_unlike/Basic mlt query with unlike",
                "msearch/*/*",
                "range/*/*",
                "scroll/*/*",
                "search/*/*",
                "search/*/*/*",
                "search/*/*/*/*",
                "search.highlight/*/*",
                "search.inner_hits/*/*",
                "search_shards/*/*",
                "search.vectors/*/*",
                "suggest/*/*",
                "tsdb/05_dimension_and_metric_in_non_tsdb_index/can't shadow dimensions",
                "tsdb/05_dimension_and_metric_in_non_tsdb_index/can't shadow metrics",

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
                "mget/14_alias_to_multiple_indices/Multi Get with alias that resolves to multiple indices",  // Also require bulk ?refresh
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

                // Require the "data" node role
                "cat.nodes/10_basic/Test cat nodes output", // Probably needs to add stateless node roles in some regexp
                "cluster.stats/10_basic/cluster stats test",
                "cluster.stats/10_basic/get cluster stats returns cluster_uuid at the top level",

                // Require the Node Stats API with index level metrics to correctly count mappings
                "nodes.stats/11_indices_metrics/indices mappings does not exist in shards level", // Failure at [nodes.stats/11_indices_metrics:540]: field [nodes.$node_id.indices.mappings.total_count] is not greater than or equal to [2]
                "nodes.stats/11_indices_metrics/indices mappings exact count test for indices level", // Failure at [nodes.stats/11_indices_metrics:502]: field [nodes.$node_id.indices.mappings.total_count] is not greater than or equal to [26]

                // Require Get API and Search API
                "termvectors/10_basic/Basic tests for termvector get",
                "mtermvectors/10_basic/Basic tests for multi termvector get",

                // Probably unsupported in stateless
                "indices.shrink/*/*",
                "indices.split/*/*",
                "tsdb/80_index_resize/clone",
                "tsdb/80_index_resize/clone no source index",
                "tsdb/80_index_resize/shrink",
                "tsdb/80_index_resize/split",

                // Require Close API
                "indices.stats/20_translog/Translog stats on closed indices", // Error: Global checkpoint [0] mismatches maximum sequence number [-1] on index shard [test][0]

            ).joinToString(",")
        )
    }
}
