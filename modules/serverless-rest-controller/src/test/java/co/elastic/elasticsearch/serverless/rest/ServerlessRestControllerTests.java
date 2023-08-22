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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class ServerlessRestControllerTests extends ESTestCase {

    private static final String SERVERLESS_INITIAL_VERSION = ServerlessRestController.VERSION_20231031;

    public static final String ACCEPT_HEADER = "Accept";
    public static final String ACCEPT_JSON_RESTV8 = "application/json; compatible-with=8";
    public static final String ACCEPT_YAML_RESTV8 = "application/yaml; compatible-with=8";

    public static final String ACCEPT_JSON_RESTV7 = "application/json; compatible-with=7";
    public static final String ACCEPT_YAML_RESTV7 = "application/yaml; compatible-with=7";

    private ThreadContext threadContext;

    @Before
    public void setup() {
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testApiVersionWithoutRequestHeader() throws Exception {
        var responseHeaders = executeRequest(Map.of());
        assertThat(responseHeaders, hasKey(ServerlessRestController.VERSION_HEADER_NAME));
        assertThat(responseHeaders.get(ServerlessRestController.VERSION_HEADER_NAME), contains(SERVERLESS_INITIAL_VERSION));
    }

    public void testApiVersionWithValidServerlessVersion() throws Exception {
        var responseHeaders = executeRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(SERVERLESS_INITIAL_VERSION)));
        assertThat(responseHeaders, hasKey(ServerlessRestController.VERSION_HEADER_NAME));
        assertThat(responseHeaders.get(ServerlessRestController.VERSION_HEADER_NAME), contains(SERVERLESS_INITIAL_VERSION));
    }

    public void testApiVersionWithV8RestCompatibilityHeader() throws Exception {
        var responseHeaders = executeRequest(Map.of(ACCEPT_HEADER, List.of(randomFrom(ACCEPT_JSON_RESTV8, ACCEPT_YAML_RESTV8))));
        assertThat(responseHeaders, not(hasKey(ServerlessRestController.VERSION_HEADER_NAME)));
    }

    public void testApiVersionWithServerlessVersionAndRestCompatibilityHeader() throws Exception {
        final String errorMessage = expectBadRequest(
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
        final String errorMessage = expectBadRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(badVersion)));
        assertThat(
            errorMessage,
            equalTo("The requested [Elastic-Api-Version] header value of [" + badVersion + "] is not valid. Only [2023-10-31] is supported")
        );
    }

    public void testApiVersionWithBadFormatServerlessVersion() throws Exception {
        final String badVersion = "v" + randomIntBetween(1, 10);
        final String errorMessage = expectBadRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(badVersion)));
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

        final String errorMessage = expectBadRequest(Map.of(ServerlessRestController.VERSION_HEADER_NAME, List.of(v1, v2)));
        assertThat(
            errorMessage,
            Matchers.either(is("The header [Elastic-Api-Version] may only be specified once. Found: [" + v1 + "],[" + v2 + "]"))
                .or(is("The header [Elastic-Api-Version] may only be specified once. Found: [" + v2 + "],[" + v1 + "]"))
        );
    }

    public void testApiVersionWithV7RestCompatibilityHeader() throws Exception {
        final String errorMessage = expectBadRequest(Map.of(ACCEPT_HEADER, List.of(randomFrom(ACCEPT_JSON_RESTV7, ACCEPT_YAML_RESTV7))));
        assertThat(errorMessage, equalTo("[compatible-with] [7] is not supported on Serverless Elasticsearch"));
    }

    private String expectBadRequest(Map<String, List<String>> requestHeaders) {
        final ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> executeRequest(requestHeaders)
        );
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        return exception.getMessage();
    }

    private Map<String, List<String>> executeRequest(Map<String, List<String>> requestHeaders) {
        final RestRequest req = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(requestHeaders).build();
        ServerlessRestController.processApiVersionHeader(req, threadContext);
        return threadContext.getResponseHeaders();
    }
}
