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

import org.elasticsearch.rest.BaseRestHandler;
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
     * Reject preferences that start with '_' because these are tightly tied to the cluster topology
     */
    private static final ParameterValidator PREFERENCE_VALIDATOR = (handler, param, value) -> {
        if (value.startsWith("_")) {
            return "The value ["
                + value
                + "] for the '"
                + param
                + "' parameter is not valid in serverless mode - preferences must not start with '_'";
        }
        return null;
    };

    /**
     * Only allow values between 5 and 64, and only for async search
     */
    private static final ParameterValidator BATCHED_REDUCE_SIZE_VALIDATOR = (handler, param, value) -> {
        if ("async_search_submit_action".equals(nameOf(handler))) {
            final int min = 5;
            final int max = 64;
            final String error = "The value ["
                + value
                + "] for the '"
                + param
                + "' parameter is not valid in serverless mode - it must be between "
                + min
                + " and "
                + max;
            try {
                int intValue = Integer.parseInt(value);
                if (intValue < min || intValue > max) {
                    return error;
                } else {
                    return null;
                }
            } catch (NumberFormatException e) {
                return error;
            }
        } else {
            return "In serverless mode, only async search requests may include the [" + param + "] parameter";
        }
    };

    private static String nameOf(RestHandler handler) {
        if (handler instanceof BaseRestHandler brh) {
            return brh.getName();
        } else {
            return "__class_" + handler.getClass().getName();
        }
    }

    /**
     * HTTP parameters that are rejected on a request to any path (if request restrictions are enabled for this request)
     */
    public static final Set<String> GLOBALLY_REJECTED_PARAMETERS = Set.of(
        "local",
        "master_timeout",
        "max_concurrent_shard_requests",
        "min_compatible_shard_node",
        "node_ids",
        "pre_filter_shard_size",
        "routing",
        "type",
        "wait_for_active_shards"
    );

    /**
     * HTTP parameters that are validated on a request to any path (if request restrictions are enabled for this request)
     */
    public static final Map<String, ParameterValidator> GLOBALLY_VALIDATED_PARAMETERS = Map.ofEntries(
        Map.entry("refresh", REJECTED_FOR_GET_DOC),
        Map.entry("preference", PREFERENCE_VALIDATOR),
        Map.entry("batched_reduce_size", BATCHED_REDUCE_SIZE_VALIDATOR)
    );

}
