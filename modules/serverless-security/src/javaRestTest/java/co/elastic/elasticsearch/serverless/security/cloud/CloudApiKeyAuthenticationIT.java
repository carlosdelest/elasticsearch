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
import co.elastic.elasticsearch.serverless.security.cloud.UniversalIamTestServer.FailedAuthenticateProjectResponse;
import co.elastic.elasticsearch.serverless.security.cloud.UniversalIamTestServer.SuccessfulAuthenticateProjectResponse;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
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
import static org.hamcrest.Matchers.not;

public class CloudApiKeyAuthenticationIT extends ESRestTestCase {

    private static final String OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_PASSWORD = "elastic-password";
    private static final String ORGANIZATION_ID = "test-org-id";
    private static final String PROJECT_ID = "test-project-id";
    private static final ProjectType PROJECT_TYPE = ProjectType.ELASTICSEARCH_GENERAL_PURPOSE;
    private static final ProjectInfo PROJECT_INFO = new ProjectInfo(PROJECT_ID, ORGANIZATION_ID, PROJECT_TYPE);

    private static final UniversalIamTestServer universalIamTestService = new UniversalIamTestServer();

    private static final ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .user(OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .setting("xpack.ml.enabled", "false")
        .setting("serverless.universal_iam_service.enabled", "true")
        .setting("serverless.universal_iam_service.url", () -> "https://localhost:" + universalIamTestService.getAddress().getPort())
        .setting("serverless.universal_iam_service.http.connect_timeout", () -> "2s")
        .setting("serverless.universal_iam_service.http.socket_timeout", () -> "2s")
        .setting("serverless.universal_iam_service.http.max_connections", () -> "10")
        .setting("serverless.universal_iam_service.ssl.truststore.path", () -> "keystore.jks")
        .setting("serverless.universal_iam_service.ssl.truststore.password", () -> "changeit")
        .setting("serverless.universal_iam_service.ssl.truststore.type", () -> "JKS")
        .setting(
            "serverless.universal_iam_service.ssl.cipher_suites",
            () -> "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        )
        .setting("serverless.universal_iam_service.ssl.supported_protocols", () -> "TLSv1.2,TLSv1.3")
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
        .configFile("keystore.jks", Resource.fromClasspath("uiam/unified-keystore.jks"))
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
    public static final TestRule rule = RuleChain.outerRule(universalIamTestService).around(cluster);

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
        final String apiKeyName = randomBoolean() ? "my-api-key-name-" + randomAlphaOfLength(20) : null;
        final String apiKeyId = "test-api-key-id";
        final String clientSharedSecret = randomBoolean() ? "shared_secret_" + randomAlphaOfLength(64) : null;
        final boolean internal = clientSharedSecret != null;
        final String apiKey = "essu_" + randomAlphaOfLength(64);
        final CloudCredentialsMetadata credentialsMetadata = new CloudCredentialsMetadata(
            "api-key",
            internal,
            Instant.now(),
            randomBoolean() ? Instant.now().plus(Duration.ofDays(7)) : null
        );
        var authenticateResponse = new SuccessfulAuthenticateProjectResponse(
            "api-key",
            apiKeyId,
            apiKeyName,
            ORGANIZATION_ID,
            credentialsMetadata,
            new CloudAuthenticateProjectContext(PROJECT_INFO, List.of("role1", "role2"))

        );
        universalIamTestService.setResponse(authenticateResponse);

        Map<String, Object> responseBody = responseAsMap(
            performRequestWithCloudApiKey(apiKey, clientSharedSecret, new Request("GET", "/_security/_authenticate"))
        );

        assertThat(ObjectPath.evaluate(responseBody, "username"), is(authenticateResponse.apiKeyId()));
        assertResponseHasRoles(responseBody, authenticateResponse.context().applicationRoles());
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.id"), is(apiKeyId));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.internal"), is(internal));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.name"), is(apiKeyName));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.managed_by"), is("cloud"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.type"), is("_cloud_api_key"));

        authenticateResponse = new SuccessfulAuthenticateProjectResponse(
            "apikey",
            apiKeyId,
            apiKeyName,
            ORGANIZATION_ID,
            credentialsMetadata,
            new CloudAuthenticateProjectContext(PROJECT_INFO, List.of())
        );
        universalIamTestService.setResponse(authenticateResponse);

        responseBody = responseAsMap(
            performRequestWithCloudApiKey(apiKey, clientSharedSecret, new Request("GET", "/_security/_authenticate"))
        );

        assertThat(ObjectPath.evaluate(responseBody, "username"), is(authenticateResponse.apiKeyId()));
        assertResponseHasNoRoles(responseBody);
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.id"), is(apiKeyId));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.internal"), is(internal));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.name"), is(apiKeyName));
        assertThat(ObjectPath.evaluate(responseBody, "api_key.managed_by"), is("cloud"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.type"), is("_cloud_api_key"));
    }

    public void testFailedAuthenticationWithCloudApiKey() throws IOException {
        final String errorMessage = "No authentication mechanism found";
        universalIamTestService.setResponse(new FailedAuthenticateProjectResponse(401, errorMessage));
        final String clientSharedSecret = randomBoolean() ? randomAlphaOfLength(64) : null;
        final String apiKey = "essu_" + randomAlphaOfLength(64);
        var e = expectThrows(
            ResponseException.class,
            () -> performRequestWithCloudApiKey(apiKey, clientSharedSecret, new Request("GET", "/_security/_authenticate"))
        );

        // TODO: adjust after improving error handling
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(401));
        assertThat(e.getMessage(), containsString("Cloud API key authentication failed."));
        assertThat(e.getMessage(), containsString("failed to authenticate cloud API key for project [" + PROJECT_ID + "]"));
        assertThat(e.getMessage(), not(containsString(errorMessage)));
    }

    public void testSearchWithCloudApiKey() throws IOException {
        final String apiKey = "essu_" + randomAlphaOfLength(64);
        final String apiKeyName = randomBoolean() ? "my-api-key-name-" + randomAlphaOfLength(20) : null;
        final String apiKeyId = "test-api-key-id";
        final String clientSharedSecret = randomBoolean() ? "shared_secret_" + randomAlphaOfLength(64) : null;
        final CloudCredentialsMetadata credentialsMetadata = new CloudCredentialsMetadata(
            "api-key",
            clientSharedSecret != null,
            Instant.now(),
            randomBoolean() ? Instant.now().plus(Duration.ofDays(7)) : null
        );

        var authenticateResponse = new SuccessfulAuthenticateProjectResponse(
            "apikey",
            apiKeyId,
            apiKeyName,
            ORGANIZATION_ID,
            credentialsMetadata,
            new CloudAuthenticateProjectContext(PROJECT_INFO, List.of("test_viewer"))

        );
        universalIamTestService.setResponse(authenticateResponse);

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
            Response searchResponse = performRequestWithCloudApiKey(apiKey, clientSharedSecret, searchRequest);
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

    private static void assertResponseHasNoRoles(Map<String, Object> responseBody) throws IOException {
        assertResponseHasRoles(responseBody, List.of());
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
}
