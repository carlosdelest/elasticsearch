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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CloudApiKeyAuthenticationIT extends ESRestTestCase {
    private static final String OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_PASSWORD = "elastic-password";
    private static final String ORGANIZATION_ID = "test-org-id";
    private static final String PROJECT_ID = "test-project-id";
    private static final ProjectType PROJECT_TYPE = ProjectType.ELASTICSEARCH_GENERAL_PURPOSE;

    private static final UniversalIamTestServer universalIamTestService = new UniversalIamTestServer();

    private static final ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .user(OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .setting("xpack.ml.enabled", "false")
        .setting("serverless.universal_iam_service.url", () -> "http://localhost:" + universalIamTestService.getAddress().getPort())
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
        var authenticateResponse = new UniversalIamTestServer.AuthenticateProjectResponse(
            "apikey",
            "test-api-key-id",
            ORGANIZATION_ID,
            "role1",
            "role2"
        );
        universalIamTestService.setResponse(authenticateResponse);

        Map<String, Object> responseBody = responseAsMap(
            performRequestWithCloudApiKey("essu_" + randomAlphaOfLength(64), new Request("GET", "/_security/_authenticate"))
        );

        assertThat(ObjectPath.evaluate(responseBody, "username"), is(authenticateResponse.apiKeyId()));
        assertResponseHasRoles(responseBody, authenticateResponse.applicationRoles());
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.type"), is("_cloud_api_key"));

        authenticateResponse = new UniversalIamTestServer.AuthenticateProjectResponse("apikey", "test-api-key-id", ORGANIZATION_ID);
        universalIamTestService.setResponse(authenticateResponse);

        responseBody = responseAsMap(
            performRequestWithCloudApiKey("essu_" + randomAlphaOfLength(64), new Request("GET", "/_security/_authenticate"))
        );

        assertThat(ObjectPath.evaluate(responseBody, "username"), is(authenticateResponse.apiKeyId()));
        assertResponseHasNoRoles(responseBody);
        assertThat(ObjectPath.evaluate(responseBody, "authentication_type"), is("api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "authentication_realm.type"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.name"), is("_cloud_api_key"));
        assertThat(ObjectPath.evaluate(responseBody, "lookup_realm.type"), is("_cloud_api_key"));
    }

    @SuppressWarnings("unchecked")
    private static void assertResponseHasRoles(Map<String, Object> responseBody, String... expectedRoles) throws IOException {
        final Object actualRoles = ObjectPath.evaluate(responseBody, "roles");
        assertThat(actualRoles, is(instanceOf(Collection.class)));
        assertThat((Collection<String>) actualRoles, containsInAnyOrder(expectedRoles));
    }

    private static void assertResponseHasNoRoles(Map<String, Object> responseBody) throws IOException {
        assertResponseHasRoles(responseBody);
    }

    private static Response performRequestWithCloudApiKey(String apiKey, Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", randomFrom("ApiKey", "apikey", "APIKEY") + " " + apiKey).build()
        );
        return client().performRequest(request);
    }
}
