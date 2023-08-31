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
        return Settings.builder()
            // .put(ThreadContext.PREFIX + ".Authorization", "ApiKey " + getApiKey())
            .put(ThreadContext.PREFIX + ".Authorization", getBasicAuthCredentials())
            .put(ThreadContext.PREFIX + ".X-Found-Cluster", getProjectId() + ".es")
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder()
            // TODO: Sort out how to use API keys since they don't support operator privileges
            // .put(ThreadContext.PREFIX + ".Authorization", "ApiKey " + getApiKey())
            .put(ThreadContext.PREFIX + ".Authorization", getBasicAuthCredentials())
            .put(ThreadContext.PREFIX + ".X-Found-Cluster", getProjectId() + ".es")
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

    protected String getProjectId() {
        String essProjectId = System.getenv().get("ESS_PROJECT_ID");
        if (essProjectId == null) {
            throw new RuntimeException("ESS_PROJECT_ID is not set");
        }
        return essProjectId;
    }

    protected String getApiKey() {
        String essApiKey = System.getenv().get("ESS_API_KEY_ENCODED");
        if (essApiKey == null) {
            throw new RuntimeException("ESS_API_KEY_ENCODED is not set");
        }
        return essApiKey;
    }

    protected String getBasicAuthCredentials() {
        String username = System.getenv().get("ESS_USERNAME");
        if (username == null) {
            throw new RuntimeException("ESS_USERNAME is not set");
        }
        String password = System.getenv().get("ESS_PASSWORD");
        if (password == null) {
            throw new RuntimeException("ESS_PASSWORD is not set");
        }

        return basicAuthHeaderValue(username, new SecureString(password));
    }
}
