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

import co.elastic.elasticsearch.serverless.rest.RestrictedRestParameters.ParameterValidator;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestInterceptor;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.MediaType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class ServerlessRestController extends RestController {

    static final String VERSION_HEADER_NAME = "Elastic-Api-Version";
    static final String VERSION_20231031 = "2023-10-31";
    static final Predicate<String> VERSION_VALIDATOR = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$").asMatchPredicate();

    private static final Logger logger = org.elasticsearch.logging.LogManager.getLogger(ServerlessRestController.class);
    private static final String PROJECT_ID_REST_HEADER = "X-Elastic-Project-Id";
    private static final String PROJECT_ID_THREADCONTEXT_HEADER = "project.id";

    // TODO: Remove this flag (and all the "log but don't fail" behaviour)
    private final boolean logValidationErrorsAsWarnings;

    public ServerlessRestController(
        RestInterceptor restInterceptor,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        Tracer tracer

    ) {
        this(restInterceptor, client, circuitBreakerService, usageService, tracer, false);
    }

    protected ServerlessRestController(
        RestInterceptor restInterceptor,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        Tracer tracer,
        boolean logValidationErrorsAsWarnings
    ) {
        super(restInterceptor, client, circuitBreakerService, usageService, tracer);
        this.logValidationErrorsAsWarnings = logValidationErrorsAsWarnings;
    }

    @Override
    protected void validateRequest(RestRequest request, RestHandler handler, NodeClient client) throws ElasticsearchStatusException {
        if (request.hasParam(RestRequest.PATH_RESTRICTED)) {
            validateRestParameters(request.path(), handler.getConcreteRestHandler(), request.params());
        }
    }

    private void validateRestParameters(String path, RestHandler handler, Map<String, String> params) throws ElasticsearchStatusException {
        Set<String> errorSet = null;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            final var paramName = entry.getKey();
            String paramError = null;
            if (RestrictedRestParameters.GLOBALLY_REJECTED_PARAMETERS.contains(paramName)) {
                paramError = "The http parameter ["
                    + paramName
                    + "] (with value ["
                    + entry.getValue()
                    + "]) is not permitted when running in serverless mode";
            } else {
                ParameterValidator validator = RestrictedRestParameters.GLOBALLY_VALIDATED_PARAMETERS.get(paramName);
                if (validator != null) {
                    paramError = validator.validate(handler, paramName, entry.getValue());
                }
            }
            if (paramError != null) {
                if (errorSet == null) {
                    errorSet = new HashSet<>();
                }
                errorSet.add(paramError);
            }
        }

        if (errorSet != null) {
            final String message = "Parameter validation failed for [" + path + "]: " + Strings.collectionToDelimitedString(errorSet, "; ");
            if (logValidationErrorsAsWarnings) {
                // Temporary behaviour to soft-launch this validation
                logger.warn(message);
            } else {
                throw new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
            }
        }
    }

    @Override
    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
        try {
            processProjectIdHeader(request, threadContext);
            processApiVersionHeader(request, threadContext);
        } catch (ElasticsearchStatusException e) {
            super.dispatchBadRequest(channel, threadContext, e);
            return;
        }
        super.dispatchRequest(request, channel, threadContext);
    }

    /**
     * This runs at the very beginning of the request dispatch, which means everything else can depend on knowing which project is being
     * referenced, but it also means that we don't know whether the user is an operator or not, so we need to defer the
     * "only operators can perform non-project operations" check to later on.
     */
    private static void processProjectIdHeader(RestRequest request, ThreadContext threadContext) {
        final String projectId = request.header(PROJECT_ID_REST_HEADER);
        if (projectId != null) {
            threadContext.putHeader(PROJECT_ID_THREADCONTEXT_HEADER, projectId);
        } else {
            // Longer term we want _every_ request to either have a project-id or a special flag for operator-only cluster-level operations
            // But doing that now would break a lot of tests and local dev, so (for now) we just log at DEBUG
            logger.debug("No project id supplied in request [{}] [{}]", request.method(), request.path());
        }
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
