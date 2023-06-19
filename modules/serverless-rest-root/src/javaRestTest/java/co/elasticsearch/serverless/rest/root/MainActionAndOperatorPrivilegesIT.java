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

package co.elasticsearch.serverless.rest.root;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MainActionAndOperatorPrivilegesIT extends ESRestTestCase {

    private static final String OPERATOR_USER = "x_pack_rest_user";
    private static final String OPERATOR_PASSWORD = "x-pack-test-password";
    private static final String NOT_OPERATOR_USER = "not_operator";
    private static final String NOT_OPERATOR_PASSWORD = "not_operator_password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("javaRestTest")
        .setting("stateless.enabled", "true")
        .setting("xpack.security.operator_privileges.enabled", "true")
        .setting("stateless.object_store.type", "fs")
        .setting("stateless.object_store.bucket", "stateless")
        .setting("stateless.object_store.base_path", "base_path")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .secret("bootstrap.password", "x-pack-test-password")
        .secret("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user(OPERATOR_USER, OPERATOR_PASSWORD)
        .user(NOT_OPERATOR_USER, NOT_OPERATOR_PASSWORD)
        .configFile("testnode.pem", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .configFile("operator_users.yml", Resource.fromClasspath("operator_users.yml"))
        .setting("ingest.geoip.downloader.enabled", "false")
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
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
        Response response = operatorClient.performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        assertThat(responseBody, containsString("\"cluster_name\""));
        assertThat(responseBody, containsString("\"name\""));
        assertThat(responseBody, containsString("\"cluster_uuid\""));
        assertThat(responseBody, containsString("\"tagline\""));
        assertThat(responseBody, containsString("\"version\""));
        assertThat(responseBody, not(containsString("\"number\"")));
        assertThat(responseBody, containsString("\"build_flavor\" : \"serverless\""));
    }

    public void testAsNotOperator() throws Exception {
        RestClient notOperatorClient = client();

        Response response = notOperatorClient.performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        assertThat(responseBody, not(containsString("\"cluster_name\"")));
        assertThat(responseBody, not(containsString("\"name\"")));
        assertThat(responseBody, not(containsString("\"cluster_uuid\"")));
        assertThat(responseBody, containsString("\"tagline\""));
        assertThat(responseBody, containsString("\"version\""));
        assertThat(responseBody, not(containsString("\"number\"")));
        assertThat(responseBody, not(containsString("\"build_hash\"")));
        assertThat(responseBody, containsString("\"build_flavor\" : \"serverless\""));
    }

}
