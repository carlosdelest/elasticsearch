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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

public class ServerlessApiVersionIT extends ESRestTestCase {

    public static final String SERVERLESS_VERSION_HEADER = "Elastic-Api-Version";
    public static final String SERVERLESS_INITIAL_VERSION = "2023-10-31";

    public static final String ACCEPT_HEADER = "Accept";
    public static final String ACCEPT_JSON_RESTV8 = "application/json; compatible-with=8";

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .user("elastic_operator", "elastic-password", User.ROOT_USER_ROLE, true)
        .user("api-client", "api-password", "superuser", false)
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("api-client", new SecureString("api-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("elastic_operator", new SecureString("elastic-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testApiVersionWithoutRequestHeader() throws Exception {
        var responseHeaders = executeRequest(Map.of());
        assertThat(responseHeaders, hasKey(SERVERLESS_VERSION_HEADER));
        assertThat(responseHeaders.get(SERVERLESS_VERSION_HEADER), contains(SERVERLESS_INITIAL_VERSION));
    }

    public void testApiVersionWithValidServerlessVersion() throws Exception {
        var responseHeaders = executeRequest(Map.of(SERVERLESS_VERSION_HEADER, SERVERLESS_INITIAL_VERSION));
        assertThat(responseHeaders, hasKey(SERVERLESS_VERSION_HEADER));
        assertThat(responseHeaders.get(SERVERLESS_VERSION_HEADER), contains(SERVERLESS_INITIAL_VERSION));
    }

    public void testApiVersionWithServerlessVersionAndRestCompatibilityHeader() throws Exception {
        var exception = expectThrows(
            ResponseException.class,
            () -> executeRequest(
                Map.ofEntries(
                    Map.entry(ACCEPT_HEADER, ACCEPT_JSON_RESTV8),
                    Map.entry(SERVERLESS_VERSION_HEADER, SERVERLESS_INITIAL_VERSION)
                )
            )
        );
        assertError(
            exception,
            400,
            "The request includes both the [Elastic-Api-Version] header and a [compatible-with] parameter,"
                + " but it is not valid to include both of these in a request"
        );
    }

    public void testApiVersionWithMultipleServerlessVersionHeadersUsingDifferentCharacterCase() throws Exception {
        final String v1 = Strings.format("%04d-%02d-%02d", randomIntBetween(2020, 2030), randomIntBetween(1, 12), randomIntBetween(1, 31));
        final String v2 = randomValueOtherThan(
            v1,
            () -> Strings.format("%04d-%02d-%02d", randomIntBetween(2020, 2030), randomIntBetween(1, 12), randomIntBetween(1, 31))
        );

        var exception = expectThrows(
            ResponseException.class,
            () -> executeRequest(
                Map.ofEntries(
                    Map.entry(SERVERLESS_VERSION_HEADER.toUpperCase(Locale.ROOT), v1),
                    Map.entry(SERVERLESS_VERSION_HEADER.toLowerCase(Locale.ROOT), v2)
                )
            )
        );
        assertError(
            exception,
            400,
            Matchers.either(is("The header [Elastic-Api-Version] may only be specified once. Found: [" + v1 + "],[" + v2 + "]"))
                .or(is("The header [Elastic-Api-Version] may only be specified once. Found: [" + v2 + "],[" + v1 + "]"))
        );
    }

    private void assertError(ResponseException exception, int expectedStatus, String expectedMessage) throws IOException {
        assertError(exception, expectedStatus, is(expectedMessage));
    }

    private void assertError(ResponseException exception, int expectedStatus, Matcher<? super String> matcher) throws IOException {
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(expectedStatus));

        final Map<String, Object> body = responseAsMap(exception.getResponse());
        assertThat(body, hasKey("error"));

        final Object reason = ObjectPath.evaluate(body, "error.reason");
        assertThat(reason, instanceOf(String.class));
        assertThat((String) reason, matcher);
    }

    private Map<String, List<String>> executeRequest(Map<String, String> requestHeaders) throws IOException {
        final Request request = new Request("GET", "/");
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        requestHeaders.forEach(options::addHeader);
        request.setOptions(options);

        final Response response = client().performRequest(request);

        return Stream.of(response.getHeaders())
            .collect(Collectors.toMap(h -> h.getName(), h -> List.of(h.getValue()), CollectionUtils::concatLists));
    }

}
