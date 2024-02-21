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

package co.elastic.elasticsearch.serverless.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ServerlessNativeUsersAndRolesIT extends ESRestTestCase {

    private static final String TEST_USER = "elastic-user";
    private static final String TEST_OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_PASSWORD = "elastic-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .settings(ServerlessNativeUsersAndRolesIT::randomisedSettings)
        .user(TEST_OPERATOR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(TEST_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, false)
        .build();

    private static Map<String, String> randomisedSettings(LocalClusterSpec.LocalNodeSpec localNodeSpec) {
        Map<String, String> settings = new HashMap<>();
        if (randomBoolean()) {
            // Verify that the native realm can be explicitly disabled (which the k8s-controller does) even if native users are disabled
            settings.put("xpack.security.authc.realms.native.disabled_native.enabled", "false");
        }
        if (randomBoolean()) {
            // Explicitly disable native user mgt, rather than relying on the serverless default
            settings.put("xpack.security.authc.native_users.enabled", "false");
        }
        if (randomBoolean()) {
            // Explicitly disable native role mgt, rather than relying on the serverless default
            settings.put("xpack.security.authc.native_roles.enabled", "false");
        }
        return settings;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_OPERATOR_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testNativeUsersNotAvailable() throws Exception {
        final ResponseException exception = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", "/_security/user"))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(410));
        assertHasErrorReasonSubstring(exception, "Native user management is not enabled");
    }

    public void testNativeRolesNotAvailable() throws Exception {
        final ResponseException exception = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", "/_security/role/" + randomAlphaOfLengthBetween(4, 8)))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(410));
        assertHasErrorReasonSubstring(exception, "Native role management is not enabled");
    }

    public void testGetRolesIsAvailableForOperators() throws Exception {
        final Response response = client().performRequest(new Request("GET", "/_security/role"));
        assertOK(response);
        final Map<String, Object> body = responseAsMap(response);
        assertThat(body, hasKey("superuser"));
    }

    public void testGetRolesNotAvailableForRegularUsers() throws Exception {
        final Request request = new Request("GET", randomFrom("/_security/role", "/_security/role/" + randomAlphaOfLengthBetween(4, 8)));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PASSWORD.toCharArray())))
        );
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(410));
        assertHasErrorReasonSubstring(exception, "Native role management is not enabled");
    }

    public void testGetBuiltinPrivilegesNotAvailableForRegularUsers() throws IOException {
        final Request request = new Request("GET", "/_security/privilege/_builtin");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PASSWORD.toCharArray())))
        );
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(410));
        assertHasErrorReasonSubstring(exception, "This API is not enabled on this Elasticsearch instance");
    }

    public void testGetBuiltinPrivilegesAvailableForOperators() throws Exception {
        final Response response = client().performRequest(new Request("GET", "/_security/privilege/_builtin"));
        assertOK(response);
        final Map<String, Object> body = responseAsMap(response);
        assertThat(body, hasKey("cluster"));
    }

    private static void assertHasErrorReasonSubstring(ResponseException exception, String errorReason) throws IOException {
        final Map<String, Object> body = responseAsMap(exception.getResponse());
        final Object reason = ObjectPath.evaluate(body, "error.reason");
        assertThat(reason, instanceOf(String.class));
        assertThat((String) reason, containsString(errorReason));
    }

}
