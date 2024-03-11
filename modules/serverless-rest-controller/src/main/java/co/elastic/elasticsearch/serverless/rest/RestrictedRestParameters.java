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

package co.elastic.elasticsearch.serverless.rest;

import java.util.Map;
import java.util.Set;

class RestrictedRestParameters {

    @FunctionalInterface
    interface ParameterValidator {

        /**
         * @return An error message if the parameter is invalid, or {@code null} if it is allowed.
         */
        String validate(String paramName, String paramValue);
    }

    /**
     * HTTP parameters that are rejected on a request to any path (if request restrictions are enabled for this request)
     */
    public static final Set<String> GLOBALLY_REJECTED_PARAMETERS = Set.of(
        "batched_reduce_size",
        "local",
        "master_timeout",
        "max_concurrent_shard_requests",
        "min_compatible_shard_node",
        "node_ids",
        "pre_filter_shard_size",
        "preference",
        "type",
        "wait_for_active_shards"
    );

    /**
     * HTTP parameters that are validated on a request to any path (if request restrictions are enabled for this request)
     */
    public static final Map<String, ParameterValidator> GLOBALLY_VALIDATED_PARAMETERS = Map.ofEntries();

}
