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

package co.elastic.elasticsearch.serverless.security.operator;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test the operator privilege is enforced in serverless.
 */
public class ServerlessOperatorPrivsIT extends ESRestTestCase {

    private static final String OPERATOR_USER = "x_pack_rest_user";
    private static final String OPERATOR_PASSWORD = "x-pack-test-password";
    private static final String NOT_OPERATOR_USER = "not_operator";
    private static final String NOT_OPERATOR_PASSWORD = "not_operator_password";

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user(OPERATOR_USER, OPERATOR_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(NOT_OPERATOR_USER, NOT_OPERATOR_PASSWORD, User.ROOT_USER_ROLE, false)
        .configFile("testnode.pem", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(NOT_OPERATOR_USER, new SecureString(NOT_OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(OPERATOR_USER, new SecureString(OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testAsOperator() throws Exception {
        RestClient operatorClient = adminClient();
        Response response = operatorClient.performRequest(new Request("GET", "/_cat/thread_pool/write"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        assertFalse(Strings.isNullOrBlank(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)));
    }

    public void testAsNotOperator() throws Exception {
        RestClient notOperatorClient = client();

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> notOperatorClient.performRequest(new Request("GET", "/_cat/thread_pool/write"))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.GONE.getStatus()));
        assertThat(
            EntityUtils.toString(exception.getResponse().getEntity(), StandardCharsets.UTF_8),
            equalTo(
                "{\"error\":{\"root_cause\":[{\"type\":\"api_not_available_exception\",\"reason\":\"Request for uri "
                    + "[/_cat/thread_pool/write] with method [GET] exists but is not available when running in serverless mode\"}],"
                    + "\"type\":\"api_not_available_exception\",\"reason\":\"Request for uri [/_cat/thread_pool/write] with method "
                    + "[GET] exists but is not available when running in serverless mode\"},\"status\":410}"
            )
        );
    }

}
