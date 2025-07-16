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

package co.elastic.elasticsearch.serverless.security.cloud;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.test.fixtures.uiam.CosmosDbEmulatorTestContainer;
import co.elastic.elasticsearch.test.fixtures.uiam.UiamServiceTestContainer;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class UniversalIamIT extends ESRestTestCase {

    private static final ProjectType PROJECT_TYPE = ProjectType.ELASTICSEARCH_GENERAL_PURPOSE;
    private static final String OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_PASSWORD = "elastic-password";
    private static final String ORGANIZATION_ID = "1";
    private static final String PROJECT_ID = "1";

    private static final Map<String, Tuple<String, String>> testApiKeys = Map.ofEntries(
        Map.entry(
            "revoked",
            new Tuple<>(
                "b1b18f35a4814a5290d9e8e446f5e6fc",
                "essu_dev_WWpGaU1UaG1NelZoTkRneE5HRTFNamt3WkRsbE9HVTBORFptTldVMlptTTZZb"
                    + "VE0WmpNMk5UY3RZelUxWmkwME1HVmpMV0l4TlRNdE1EZGlOamsyTkdVNE5tRm0AAAAAZ+1AHg=="
            )
        ),
        Map.entry(
            "internal",
            new Tuple<>(
                "f5b6bfd76d7a447597939fcc29ca1a63",
                "essu_dev_WmpWaU5tSm1aRGMyWkRkaE5EUTNOVGszT1RNNVptTmpNamxqWVRGaE5qTT"
                    + "ZNMkZqWWpBME16QXRPV1ZpTUMwMFpHWXhMV0poTWpndFpUVXhaamxsWlRjMFlqazAAAAAAP3YUwQ=="
            )
        ),
        Map.entry(
            "test_viewer",
            new Tuple<>(
                "dced80ca157f45b6aab11f6c0ab9c5ae",
                "essu_dev_WkdObFpEZ3dZMkV4TlRkbU5EVmlObUZoWWpFeFpqWmpNR0ZpT1dNMVlXVTZ"
                    + "NemsyWXpWbVpUWXRNVFl5TUMwMFpERXhMVGsxT1RjdFl6UmhaR0kxTTJGaVpqbGgAAAAAa0jjHg=="
            )
        ),
        Map.entry(
            "admin",
            new Tuple<>(
                "a97654adbb804e48a6f12f1e6fe7dd8b",
                "essu_dev_WVRrM05qVTBZV1JpWWpnd05HVTBPR0UyWmpFeVpqRmxObVpsTjJSa09HSTZ"
                    + "ZemhsTTJZeU9Ea3ROakV5WWkwMFpqWTBMV0ptTm1VdFlqSm1abVk0T0RkaU1qWTUAAAAAlM/ZAA=="
            )
        )

    );
    private static final CosmosDbEmulatorTestContainer cosmosDbContainer = new CosmosDbEmulatorTestContainer(
        getResourcePath("uiam/test-api-keys.json")
    );

    private static final UiamServiceTestContainer uiamContainer = new UiamServiceTestContainer(cosmosDbContainer);

    private static final ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("uiam-it-tests")
        .user(OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .setting("xpack.ml.enabled", "false")
        .setting("serverless.universal_iam_service.enabled", "true")
        .setting("serverless.universal_iam_service.url", uiamContainer::getUiamUrl)
        .setting("serverless.universal_iam_service.http.connect_timeout", "2s")
        .setting("serverless.universal_iam_service.http.socket_timeout", "30s")
        .setting("serverless.universal_iam_service.http.max_connections", "5")
        // Disable SSL verification for now
        .setting("serverless.universal_iam_service.ssl.verification_mode", "none")
        // TODO we don't want metering for this test, however, metering is automatically enabled if project_id is set. furthermore,
        // metering uses a default https address which makes boot fail
        // we should fix this to allow disabling metering via a metering-specific setting (e.g., metering.url)
        // for now, just set a bogus http address since it lets the boot succeed and does not break the test
        .setting("metering.url", "http://localhost:" + "1234")
        .setting("metering.report_period", "10m")
        .setting("serverless.autoscaling.search_metrics.push_interval", "10m")
        .setting("serverless.project_type", PROJECT_TYPE.name())
        .setting("serverless.project_id", PROJECT_ID)
        .setting("serverless.organization_id", ORGANIZATION_ID)
        .setting("logger.co.elastic.elasticsearch.serverless.security", "TRACE")
        .rolesFile(Resource.fromString("""
            admin:
              cluster: [ "all" ]
              indices:
              - names:  [ "*" ]
                privileges:  [ "all" ]
              metadata:
                _public: true
                _reserved: true
            test_viewer:
              cluster: [ "all" ]
              indices:
              - names:  [ "test*" ]
                privileges:  [ "read" ]
              metadata:
                _public: true
                _reserved: true
            """))
        .build();

    @ClassRule
    public static final TestRule rule = RuleChain.outerRule(cosmosDbContainer).around(uiamContainer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(OPERATOR_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testAuthenticateWithCloudApiKey() throws IOException {
        final String apiKey = testApiKeys.get("test_viewer").v2();
        final String apiKeyId = testApiKeys.get("test_viewer").v1();

        var responseBody = responseAsMap(performRequestWithCloudApiKey(apiKey, new Request("GET", "/_security/_authenticate")));

        assertThat(ObjectPath.evaluate(responseBody, "username"), is(apiKeyId));
        assertResponseHasRoles(responseBody, List.of("test_viewer"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.id"), is(apiKeyId));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.internal"), is(false));
        // TODO: API key name is not yet returned by the UIAM service
        // assertThat(ObjectPath.evaluate(responseBody, "api_key.name"), is("test-viewer-api-key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.managed_by"), is("cloud"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.type"), is("_cloud_api_key"));
    }

    public void testAuthenticateWithInternalCloudApiKey() throws IOException {
        final String apiKey = testApiKeys.get("internal").v2();
        final String apiKeyId = testApiKeys.get("internal").v1();
        final String sharedSecret = uiamContainer.getSharedSecret();

        var responseBody = responseAsMap(
            performRequestWithCloudApiKey(apiKey, sharedSecret, new Request("GET", "/_security/_authenticate"))
        );

        assertThat(ObjectPath.evaluate(responseBody, "username"), is(apiKeyId));
        assertResponseHasRoles(responseBody, List.of("superuser"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.id"), is(apiKeyId));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.internal"), is(true));
        // TODO: API key name is not yet returned by the UIAM service
        // assertThat(ObjectPath.evaluate(responseBody, "api_key.name"), is("test-internal-api-key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.managed_by"), is("cloud"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.type"), is("_cloud_api_key"));
    }

    public void testAuthenticateWithInternalCloudApiKeyFailsWithoutSharedSecret() {
        final String apiKey = testApiKeys.get("internal").v2();

        var e = expectThrows(
            ResponseException.class,
            () -> performRequestWithCloudApiKey(apiKey, new Request("GET", "/_security/_authenticate"))
        );

        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        assertThat(e.getMessage(), containsString("failed to authenticate cloud API key"));
        assertThat(e.getMessage(), containsString("failed to authenticate cloud API key for project [" + PROJECT_ID + "]"));
    }

    public void testFailedAuthenticationWithCloudApiKey() {
        final String apiKey = "essu_invalid_" + randomAlphaOfLength(64);
        var e = expectThrows(
            ResponseException.class,
            () -> performRequestWithCloudApiKey(apiKey, new Request("GET", "/_security/_authenticate"))
        );

        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        assertThat(e.getMessage(), containsString("failed to authenticate cloud API key"));
        assertThat(e.getMessage(), containsString("failed to authenticate cloud API key for project [" + PROJECT_ID + "]"));
    }

    public void testSearchWithCloudApiKey() throws IOException {
        final String apiKey = testApiKeys.get("test_viewer").v2();
        final String apiKeyId = testApiKeys.get("test_viewer").v1();

        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "test-index-1" } }
            { "foo": "bar" }
            { "index": { "_index": "test-index-2" } }
            { "bar": "foo" }
            { "index": { "_index": "other-index" } }
            { "baz": "fee" }
            """));
        assertOK(adminClient().performRequest(bulkRequest));

        {
            Request searchRequest = new Request("POST", "/*/_search");
            Response searchResponse = performRequestWithCloudApiKey(apiKey, searchRequest);
            assertSearchResponseContainsIndices(searchResponse, Set.of("test-index-1", "test-index-2"));
        }
        {
            Request searchRequest = new Request("POST", "/other-index/_search");
            var e = expectThrows(ResponseException.class, () -> performRequestWithCloudApiKey(apiKey, searchRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                e.getMessage(),
                containsString(
                    "action [indices:data/read/search] is unauthorized for cloud API key ["
                        + apiKeyId
                        + "] with effective roles [test_viewer] on indices [other-index], "
                        + "this action is granted by the index privileges [read,all]"
                )
            );
        }
    }

    protected void assertSearchResponseContainsIndices(Response searchResponse, Set<String> expectedIndices) {
        try {
            assertOK(searchResponse);
            var response = SearchResponseUtils.responseAsSearchResponse(searchResponse);
            try {
                final var searchResult = Arrays.stream(response.getHits().getHits())
                    .collect(Collectors.toMap(SearchHit::getIndex, SearchHit::getSourceAsMap));

                assertThat(searchResult.keySet(), equalTo(expectedIndices));
            } finally {
                response.decRef();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertResponseHasRoles(Map<String, Object> responseBody, List<String> expectedRoles) throws IOException {
        final Object actualRoles = ObjectPath.evaluate(responseBody, "roles");
        assertThat(actualRoles, is(instanceOf(Collection.class)));
        assertThat((Collection<String>) actualRoles, containsInAnyOrder(expectedRoles.toArray(new String[0])));
    }

    private static Response performRequestWithCloudApiKey(String apiKey, Request request) throws IOException {
        return performRequestWithCloudApiKey(apiKey, null, request);
    }

    private static Response performRequestWithCloudApiKey(String apiKey, String sharedSecret, Request request) throws IOException {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("Authorization", randomFrom("ApiKey", "apikey", "APIKEY") + " " + apiKey);
        if (sharedSecret != null) {
            options.addHeader(CloudApiKeyAuthenticator.CLIENT_AUTHENTICATION_HEADER, sharedSecret);
        }
        request.setOptions(options.build());
        return client().performRequest(request);
    }

    private static Path getResourcePath(String path) {
        final URL resource = UniversalIamIT.class.getClassLoader().getResource(path);
        try {
            return PathUtils.get(resource.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError("Failed to build resource path for [" + path + "]", e);
        }
    }
}
