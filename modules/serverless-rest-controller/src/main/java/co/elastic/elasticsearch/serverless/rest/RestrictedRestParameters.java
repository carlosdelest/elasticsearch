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

import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.action.document.RestGetAction;

import java.util.Map;
import java.util.Set;

class RestrictedRestParameters {

    @FunctionalInterface
    interface ParameterValidator {

        /**
         * @return An error message if the parameter is invalid, or {@code null} if it is allowed.
         */
        String validate(RestHandler handler, String paramName, String paramValue);
    }

    /**
     * Reject the supplied parameter if it applied to the {@link RestGetAction} handler.
     */
    private static final ParameterValidator REJECTED_FOR_GET_DOC = (handler, param, value) -> {
        if (handler instanceof RestGetAction) {
            return "In serverless mode, get requests may not include the [" + param + "] parameter";
        }
        return null;
    };

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
        "routing",
        "type",
        "wait_for_active_shards"
    );

    /**
     * HTTP parameters that are validated on a request to any path (if request restrictions are enabled for this request)
     */
    public static final Map<String, ParameterValidator> GLOBALLY_VALIDATED_PARAMETERS = Map.ofEntries(
        Map.entry("refresh", REJECTED_FOR_GET_DOC)
    );

}
