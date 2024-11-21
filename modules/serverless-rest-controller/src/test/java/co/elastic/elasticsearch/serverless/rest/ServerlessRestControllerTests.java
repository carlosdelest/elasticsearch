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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class ServerlessRestControllerTests extends ESTestCase {

    private static final String SERVERLESS_INITIAL_VERSION = ServerlessRestController.VERSION_20231031;

    public static final String ACCEPT_HEADER = "Accept";
    public static final String ACCEPT_JSON_RESTV8 = "application/vnd.elasticsearch+json;compatible-with=8";
    public static final String ACCEPT_YAML_RESTV8 = "application/vnd.elasticsearch+yaml;compatible-with=8";
    public static final String ACCEPT_JSON_RESTV9 = "application/vnd.elasticsearch+json;compatible-with=9";
    public static final String ACCEPT_YAML_RESTV9 = "application/vnd.elasticsearch+yaml;compatible-with=9";

    private ThreadContext threadContext;
    private NoOpNodeClient client;
    private ServerlessRestController controller;
    private RestHandler restHandler;

    @Before
    public void setup() {
        final TestThreadPool threadPool = createThreadPool();
        this.threadContext = threadPool.getThreadContext();
        this.client = new NoOpNodeClient(threadPool);
        this.controller = new ServerlessRestController(
            null,
            client,
            mock(CircuitBreakerService.class),
            mock(UsageService.class),
            TelemetryProvider.NOOP,
            false
        );
        this.restHandler = new DummyRestHandler(randomAlphaOfLength(3) + "_" + randomAlphaOfLength(5));
    }

    @After
    public void teardown() {
        this.client.threadPool().shutdown();
    }

    public void testApiVersionWithoutRequestHeader() throws Exception {
        var responseHeaders = processApiVersionRequest(Map.of());
        assertThat(responseHeaders, hasKey(ServerlessRestController.VERSION_HEADER_NAME));
        assertThat(responseHeaders.get(ServerlessRestController.VERSION_HEADER_NAME), contains(SERVERLESS_INITIAL_VERSION));
    }

    public void testApiVersionWithValidServerlessVersion() throws Exception {
        var responseHeaders = processApiVersionRequest(
            Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(SERVERLESS_INITIAL_VERSION))
        );
        assertThat(responseHeaders, hasKey(ServerlessRestController.VERSION_HEADER_NAME));
        assertThat(responseHeaders.get(ServerlessRestController.VERSION_HEADER_NAME), contains(SERVERLESS_INITIAL_VERSION));
    }

    public void testApiVersionWithSupportedRestCompatibilityHeader() throws Exception {
        var responseHeaders = processApiVersionRequest(
            Map.of(ACCEPT_HEADER, List.of(randomFrom(ACCEPT_JSON_RESTV8, ACCEPT_YAML_RESTV8, ACCEPT_JSON_RESTV9, ACCEPT_YAML_RESTV9)))
        );
        assertThat(responseHeaders, not(hasKey(ServerlessRestController.VERSION_HEADER_NAME)));
    }

    public void testApiVersionWithServerlessVersionAndRestCompatibilityHeader() throws Exception {
        final String errorMessage = expectBadApiVersionRequest(
            Map.ofEntries(
                Map.entry(ACCEPT_HEADER, List.of(randomFrom(ACCEPT_JSON_RESTV8, ACCEPT_YAML_RESTV8))),
                Map.entry(ServerlessRestController.VERSION_HEADER_NAME, List.of(SERVERLESS_INITIAL_VERSION))
            )
        );
        assertThat(
            errorMessage,
            equalTo(
                "The request includes both the [Elastic-Api-Version] header and a [compatible-with] parameter,"
                    + " but it is not valid to include both of these in a request"
            )
        );
    }

    public void testApiVersionWithInvalidServerlessVersion() throws Exception {
        final String badVersion = randomValueOtherThan(
            SERVERLESS_INITIAL_VERSION,
            () -> Strings.format("%04d-%02d-%02d", randomIntBetween(2020, 2030), randomIntBetween(1, 12), randomIntBetween(1, 31))
        );
        final String errorMessage = expectBadApiVersionRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(badVersion)));
        assertThat(
            errorMessage,
            equalTo("The requested [Elastic-Api-Version] header value of [" + badVersion + "] is not valid. Only [2023-10-31] is supported")
        );
    }

    public void testApiVersionWithBadFormatServerlessVersion() throws Exception {
        final String badVersion = "v" + randomIntBetween(1, 10);
        final String errorMessage = expectBadApiVersionRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(badVersion)));
        assertThat(
            errorMessage,
            equalTo(
                "The requested [Elastic-Api-Version] header value of ["
                    + badVersion
                    + "] is not in the correct format."
                    + " Versions must be in the form [YYYY-MM-DD], for example [2023-10-31]"
            )
        );
    }

    public void testApiVersionWithMultipleServerlessVersionHeaders() throws Exception {
        final String v1 = Strings.format("%04d-%02d-%02d", randomIntBetween(2020, 2030), randomIntBetween(1, 12), randomIntBetween(1, 31));
        final String v2 = randomValueOtherThan(
            v1,
            () -> Strings.format("%04d-%02d-%02d", randomIntBetween(2020, 2030), randomIntBetween(1, 12), randomIntBetween(1, 31))
        );

        final String errorMessage = expectBadApiVersionRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(v1, v2)));
        assertThat(
            errorMessage,
            Matchers.either(is("The header [Elastic-Api-Version] may only be specified once. Found: [" + v1 + "],[" + v2 + "]"))
                .or(is("The header [Elastic-Api-Version] may only be specified once. Found: [" + v2 + "],[" + v1 + "]"))
        );
    }

    public void testRejectedHttpParametersForNonOperator() throws Exception {
        final List<String> invalidParamNames = randomNonEmptySubsetOf(RestrictedRestParameters.GLOBALLY_REJECTED_PARAMETERS);
        final List<String> validParamNames = randomList(
            1,
            5,
            () -> randomValueOtherThanMany(
                name -> RestrictedRestParameters.GLOBALLY_REJECTED_PARAMETERS.contains(name)
                    || RestrictedRestParameters.GLOBALLY_VALIDATED_PARAMETERS.containsKey(name),
                () -> randomAlphaOfLengthBetween(3, 12)
            )
        );
        final Map<String, String> parameters = Stream.concat(invalidParamNames.stream(), validParamNames.stream())
            .collect(Collectors.toMap(Function.identity(), key -> randomAlphaOfLengthBetween(2, 8)));
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String errorMessage = expectParameterValidationFailure(path, parameters);
        assertThat(errorMessage, startsWith("Parameter validation failed for [" + path + "]: "));
        for (String param : invalidParamNames) {
            assertThat(
                errorMessage,
                containsString(
                    "The http parameter ["
                        + param
                        + "] (with value ["
                        + parameters.get(param)
                        + "]) is not permitted when running in serverless mode"
                )
            );
        }
        for (String param : validParamNames) {
            assertThat(
                errorMessage,
                not(containsString("The http parameter [" + param + "] is not permitted when running in serverless mode"))
            );
        }
    }

    public void testRejectRefreshParameterOnGetDoc() throws Exception {
        this.restHandler = new RestGetAction();
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String errorMessage = expectParameterValidationFailure(path, Map.of("refresh", String.valueOf(randomBoolean())));
        assertThat(
            errorMessage,
            equalTo(
                "Parameter validation failed for [" + path + "]: In serverless mode, get requests may not include the [refresh] parameter"
            )
        );
    }

    public void testValidationErrorForPreferenceParameterWithNonOperator() throws Exception {
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String preferenceValue = "_" + randomAlphaOfLengthBetween(2, 8);
        final Map<String, String> parameters = Map.of("preference", preferenceValue);
        final String errorMessage = expectParameterValidationFailure(path, parameters);
        assertThat(
            errorMessage,
            is(
                "Parameter validation failed for ["
                    + path
                    + "]: The value ["
                    + preferenceValue
                    + "] for the 'preference' parameter is not valid in serverless mode - preferences must not start with '_'"
            )
        );
    }

    public void testValidationSuccessForPreferenceParameterWithNonOperator() throws Exception {
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String preferenceValue = randomAlphaOfLengthBetween(2, 8);
        final Map<String, String> parameters = Map.of("preference", preferenceValue);
        try {
            validateRequest(path, parameters, false);
        } catch (ElasticsearchStatusException e) {
            fail("Expected validation of parameter value [" + preferenceValue + "] to succeed, but failed with " + e);
        }
    }

    public void testValidationErrorForBatchedReduceSizeParameterWithNonOperatorOnAsyncSearch() throws Exception {
        this.restHandler = new DummyRestHandler("async_search_submit_action");
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String parameterValue;
        if (randomBoolean()) {
            parameterValue = randomAlphaOfLengthBetween(2, 8);
        } else {
            parameterValue = Integer.toString(randomBoolean() ? randomIntBetween(1, 4) : randomIntBetween(65, Integer.MAX_VALUE));
        }
        final Map<String, String> parameters = Map.of("batched_reduce_size", parameterValue);
        final String errorMessage = expectParameterValidationFailure(path, parameters);
        assertThat(
            errorMessage,
            is(
                "Parameter validation failed for ["
                    + path
                    + "]: The value ["
                    + parameterValue
                    + "] for the 'batched_reduce_size' parameter is not valid in serverless mode - it must be between 5 and 64"
            )
        );
    }

    public void testValidationSuccessForBatchedReduceSizeParameterWithNonOperatorOnAsyncSearch() throws Exception {
        this.restHandler = new DummyRestHandler("async_search_submit_action");
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String parameterValue = String.valueOf(randomIntBetween(5, 64));
        final Map<String, String> parameters = Map.of("batched_reduce_size", parameterValue);
        try {
            validateRequest(path, parameters, false);
        } catch (ElasticsearchStatusException e) {
            fail("Expected validation of parameter value [" + parameterValue + "] to succeed, but failed with " + e);
        }
    }

    public void testValidationErrorForBatchedReduceSizeParameterWithNonOperatorOnRegularSearch() throws Exception {
        this.restHandler = new DummyRestHandler("search_action");
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final String parameterValue = String.valueOf(randomIntBetween(5, 64));
        final Map<String, String> parameters = Map.of("batched_reduce_size", parameterValue);
        final String errorMessage = expectParameterValidationFailure(path, parameters);
        assertThat(
            errorMessage,
            is(
                "Parameter validation failed for ["
                    + path
                    + "]: In serverless mode, only async search requests may include the [batched_reduce_size] parameter"
            )
        );
    }

    public void testAllHttpParametersAllowedForOperator() throws Exception {
        if (randomBoolean()) {
            this.restHandler = new RestGetAction();
        } else {
            this.restHandler = new DummyRestHandler(randomAlphaOfLength(3) + "_" + randomAlphaOfLength(5));
        }
        final String path = "/" + randomAlphaOfLength(3) + "/" + randomAlphaOfLength(5);
        final List<String> operatorOnlyParamNames = randomNonEmptySubsetOf(RestrictedRestParameters.GLOBALLY_REJECTED_PARAMETERS);
        final List<String> validatedParamNames = randomSubsetOf(RestrictedRestParameters.GLOBALLY_VALIDATED_PARAMETERS.keySet());
        final List<String> permittedParamNames = randomList(
            1,
            5,
            () -> randomValueOtherThanMany(
                name -> RestrictedRestParameters.GLOBALLY_REJECTED_PARAMETERS.contains(name)
                    || RestrictedRestParameters.GLOBALLY_VALIDATED_PARAMETERS.containsKey(name),
                () -> randomAlphaOfLengthBetween(3, 12)
            )
        );
        Map<String, String> parameters = new HashMap<>();
        Stream.concat(operatorOnlyParamNames.stream(), Stream.concat(validatedParamNames.stream(), permittedParamNames.stream()))
            .forEach(name -> parameters.put(name, (randomBoolean() ? "_" : "") + randomAlphaOfLengthBetween(2, 8)));

        assertThat(parameters.size(), equalTo(operatorOnlyParamNames.size() + validatedParamNames.size() + permittedParamNames.size()));
        validateRequest(path, parameters, true);
    }

    private String expectBadApiVersionRequest(Map<String, List<String>> requestHeaders) {
        final ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> processApiVersionRequest(requestHeaders)
        );
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        return exception.getMessage();
    }

    private Map<String, List<String>> processApiVersionRequest(Map<String, List<String>> requestHeaders) {
        final RestRequest req = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(requestHeaders).build();
        ServerlessRestController.processApiVersionHeader(req, threadContext);
        return threadContext.getResponseHeaders();
    }

    private String expectParameterValidationFailure(String path, Map<String, String> requestParameters) {
        final ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> validateRequest(path, requestParameters, false)
        );
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        return exception.getMessage();
    }

    private void validateRequest(String path, Map<String, String> requestParameters, boolean isOperator) {
        final RestRequest req = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath(path)
            .withParams(new HashMap<>(requestParameters))
            .build();
        req.markAsServerlessRequest();
        if (isOperator) {
            req.markAsOperatorRequest();
        }
        controller.validateRequest(req, restHandler, client);
    }

    private class DummyRestHandler extends BaseRestHandler {

        private final String name;

        private DummyRestHandler(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public List<Route> routes() {
            return List.of();
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            // no-op
            return ignore -> {};
        }

    }
}
