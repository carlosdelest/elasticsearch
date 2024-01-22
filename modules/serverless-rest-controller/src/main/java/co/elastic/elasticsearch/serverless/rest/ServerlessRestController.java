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

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.MediaType;

import java.util.function.Predicate;
import java.util.regex.Pattern;

public class ServerlessRestController extends RestController {
    static final String VERSION_HEADER_NAME = "Elastic-Api-Version";
    static final String VERSION_20231031 = "2023-10-31";
    static final Predicate<String> VERSION_VALIDATOR = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$").asMatchPredicate();

    public ServerlessRestController(
        RestInterceptor restInterceptor,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        Tracer tracer
    ) {
        super(restInterceptor, client, circuitBreakerService, usageService, tracer);
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        try {
            processApiVersionHeader(request, threadContext);
        } catch (ElasticsearchStatusException e) {
            super.dispatchBadRequest(channel, threadContext, e);
            return;
        }
        super.dispatchRequest(request, channel, threadContext);
    }

    static void processApiVersionHeader(RestRequest request, ThreadContext threadContext) throws ElasticsearchStatusException {
        final String requestedVersion = readVersionHeader(request);
        if (request.getRestApiVersion().major < 8) {
            throw badRequest(
                "["
                    + MediaType.COMPATIBLE_WITH_PARAMETER_NAME
                    + "] ["
                    + request.getRestApiVersion().major
                    + "] is not supported on Serverless Elasticsearch"
            );
        }
        if (requestedVersion == null) {
            if (request.hasExplicitRestApiVersion() == false) {
                // If the request does not have an explicit version (neither serverless nor rest-compatibility)
                // then the response will be in the latest version, and the response header should indicate that
                threadContext.addResponseHeader(VERSION_HEADER_NAME, VERSION_20231031);
            }
        } else if (request.hasExplicitRestApiVersion()) {
            throw badRequest(
                "The request includes both the ["
                    + VERSION_HEADER_NAME
                    + "] header and a ["
                    + MediaType.COMPATIBLE_WITH_PARAMETER_NAME
                    + "] parameter, but it is not valid to include both of these in a request"
            );
        } else if (requestedVersion.equals(VERSION_20231031) == false) {
            if (VERSION_VALIDATOR.test(requestedVersion)) {
                throw badRequest(
                    "The requested ["
                        + VERSION_HEADER_NAME
                        + "] header value of ["
                        + requestedVersion
                        + "] is not valid. Only ["
                        + VERSION_20231031
                        + "] is supported"
                );
            } else {
                throw badRequest(
                    "The requested ["
                        + VERSION_HEADER_NAME
                        + "] header value of ["
                        + requestedVersion
                        + "] is not in the correct format. Versions must be in the form [YYYY-MM-DD], for example ["
                        + VERSION_20231031
                        + "]"
                );
            }
        } else {
            // A serverless version header was included, the rest compatibility header was not included & the API version is a valid value
            threadContext.addResponseHeader(VERSION_HEADER_NAME, requestedVersion);
        }
    }

    private static String readVersionHeader(RestRequest request) {
        var values = request.getAllHeaderValues(VERSION_HEADER_NAME);
        if (values == null || values.isEmpty()) {
            return null;
        }
        if (values.size() > 1) {
            throw badRequest(
                "The header [" + VERSION_HEADER_NAME + "] may only be specified once. Found: [" + String.join("],[", values) + "]"
            );
        }
        return values.get(0);
    }

    private static ElasticsearchStatusException badRequest(String msg) {
        return new ElasticsearchStatusException(msg, RestStatus.BAD_REQUEST);
    }

}
