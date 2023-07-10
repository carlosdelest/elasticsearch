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

package co.elastic.elasticsearch.qa.e2e;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;

public abstract class AbstractServerlessE2eTest extends ESRestTestCase {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.remote().build();

    protected void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        super.configureClient(builder, settings);
        disableTlsVerification(builder);
    }

    private static void disableTlsVerification(RestClientBuilder builder) {
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            try {
                SSLContext sslContext = SSLContexts.custom()
                    .loadTrustMaterial(null, (X509Certificate[] chain, String authType) -> true)
                    .build();
                httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                httpClientBuilder.setSSLContext(sslContext);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            } catch (KeyManagementException e) {
                throw new RuntimeException(e);
            } catch (KeyStoreException e) {
                throw new RuntimeException(e);
            }
            return httpClientBuilder;
        });
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(getUserName(), new SecureString(getUserPassword().toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ThreadContext.PREFIX + ".X-elastic-internal-origin", true)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(getUserName(), new SecureString(getUserPassword().toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ThreadContext.PREFIX + ".X-elastic-internal-origin", true)
            .put("client.path.prefix", "/")
            .build();
    }

    /**
     * Overriding this to switch to https.
     */
    protected String getProtocol() {
        return "https";
    }

    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public String getUserName() {
        String essTestUsername = System.getenv().get("ESS_TEST_USERNAME");
        if (essTestUsername == null) {
            throw new RuntimeException("ESS_TEST_USERNAME is not set");
        }
        return essTestUsername;
    }

    public String getUserPassword() {
        String essTestUserpassword = System.getenv().get("ESS_TEST_PASSWORD");
        if (essTestUserpassword == null) {
            throw new RuntimeException("ESS_TEST_PASSWORD is not set");
        }
        return essTestUserpassword;

    }
}
