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

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin.UNIVERSAL_IAM_SERVICE_URL_SETTING;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public class UniversalIamClient implements Closeable {

    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 1000;
    public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 30000;
    public static final int DEFAULT_MAX_CONNECTIONS = 30;
    public static final int DEFAULT_SSL_HANDSHAKE_TIMEOUT_MINUTES = 1;
    public static final int DEFAULT_TTL_MINUTES = 10;

    public static final String SETTING_PREFIX_UIAM = "serverless.universal_iam_service.";
    public static final Setting<org.elasticsearch.core.TimeValue> CONNECT_TIMEOUT = timeSetting(
        SETTING_PREFIX_UIAM + "http.connect_timeout",
        new org.elasticsearch.core.TimeValue(DEFAULT_CONNECT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
        Setting.Property.NodeScope
    );
    public static final Setting<org.elasticsearch.core.TimeValue> SOCKET_TIMEOUT = timeSetting(
        SETTING_PREFIX_UIAM + "http.socket_timeout",
        new org.elasticsearch.core.TimeValue(DEFAULT_SOCKET_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
        Setting.Property.NodeScope
    );
    public static final Setting<Integer> MAX_CONNECTIONS = Setting.intSetting(
        SETTING_PREFIX_UIAM + "http.max_connections",
        DEFAULT_MAX_CONNECTIONS,
        1,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(UniversalIamClient.class);
    private static final String BASE_UIAM_PATH = "/uiam/api/v1";
    private static final String AUTHENTICATE_PROJECT_ENDPOINT = BASE_UIAM_PATH + "/authentication/_authenticate-project";
    private static final String HEADER_X_CLIENT_AUTHENTICATION = "X-Client-Authentication";
    private final UniversalIamSslConfig sslConfig;

    private final URI authenticateProjectUri;
    private final CloseableHttpAsyncClient httpClient;

    public UniversalIamClient(Settings settings, UniversalIamSslConfig sslConfig) {
        this.authenticateProjectUri = UNIVERSAL_IAM_SERVICE_URL_SETTING.get(settings).resolve(AUTHENTICATE_PROJECT_ENDPOINT);
        this.sslConfig = sslConfig;
        this.httpClient = createHttpClient(settings, sslConfig);

    }

    /**
     * Sends the authentication request to the universal IAM service.
     * <p>
     * The authentication request is expected to return a 200 response containing the authenticated user information,
     * or a 401 error if the authentication fails. Every other response is considered an internal error.
     */
    public void authenticateProject(CloudApiKeyAuthenticationRequest request, ActionListener<CloudApiKeyAuthenticationResponse> listener) {
        final SimpleHttpRequest httpGet = toSimpleHttpGetRequest(authenticateProjectUri, request);

        logger.debug("Authenticating against universal IAM service [{}]", httpGet.getRequestUri());

        httpClient.execute(httpGet, new FutureCallback<>() {

            @Override
            public void completed(final SimpleHttpResponse result) {
                logger.debug("cloud API key authentication request against universal IAM service [{}] succeeded", httpGet.getRequestUri());
                handleResponse(result, listener);
            }

            @Override
            public void failed(Exception e) {
                listener.onFailure(
                    new IllegalStateException("cloud API key authentication request against universal IAM service failed", e)
                );
            }

            @Override
            public void cancelled() {
                listener.onFailure(
                    new IllegalStateException("cloud API key authentication request against universal IAM service was cancelled")
                );
            }
        });
    }

    private void handleResponse(SimpleHttpResponse httpResponse, ActionListener<CloudApiKeyAuthenticationResponse> listener) {

        if (false == RestStatus.isSuccessful(httpResponse.getCode())) {
            final String responseContent = httpResponse.getBodyText();
            // TODO better failure handling -- consider parsing error details (if available) and tailoring message
            if (httpResponse.getCode() == RestStatus.UNAUTHORIZED.getStatus()) {
                logger.debug(
                    "Cloud API key authentication request failed with status [{}] and response [{}]",
                    httpResponse.getCode(),
                    responseContent
                );
                listener.onFailure(new ElasticsearchSecurityException("Cloud API key authentication failed."));
            } else {
                // everything else is considered as unexpected (i.e. internal server error)
                logger.error(
                    "Cloud API key authentication request failed with unexpected status [{}] and response [{}]",
                    httpResponse.getCode(),
                    responseContent
                );
                listener.onFailure(new IllegalStateException("Received unexpected response while authenticating cloud API key"));
            }
            return;
        }

        final ContentType contentTypeHeader = httpResponse.getContentType();
        final String contentTypeValue = contentTypeHeader == null ? null : contentTypeHeader.getMimeType();
        if (false == ContentType.APPLICATION_JSON.getMimeType().equals(contentTypeValue)) {
            listener.onFailure(
                new IllegalStateException(
                    "unable to parse response. Content type was expected to be [application/json] but was [" + contentTypeValue + "]"
                )
            );
            return;
        }

        try {
            listener.onResponse(CloudApiKeyAuthenticationResponse.parse(httpResponse.getBodyBytes()));
        } catch (Exception e) {
            listener.onFailure(new IllegalStateException("Failed to parse the response from universal IAM service", e));
        }
    }

    protected XContentParser createJsonParser(byte[] data) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, data);
    }

    private static SimpleHttpRequest toSimpleHttpGetRequest(URI baseUri, CloudApiKeyAuthenticationRequest request) {
        final URI fullUri;
        final ProjectInfo projectInfo = request.projectInfo();
        final CloudApiKey cloudApiKey = request.cloudApiKey();
        try {
            fullUri = new URIBuilder(baseUri).addParameter("project_id", projectInfo.projectId())
                .addParameter("project_owner", projectInfo.organizationId())
                .addParameter("project_type", projectInfo.projectType())
                .build();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("failed to create full URI for universal IAM service", e);
        }
        SimpleHttpRequest simpleRequest = SimpleHttpRequest.create(Method.GET, fullUri);
        simpleRequest.setHeader(HttpHeaders.AUTHORIZATION, ApiKeyService.withApiKeyPrefix(cloudApiKey.cloudApiKeyCredentials().toString()));
        return simpleRequest;
    }

    private static CloseableHttpAsyncClient createHttpClient(Settings settings, UniversalIamSslConfig sslConfig) {
        PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setTlsStrategy(sslConfig.getStrategy())
            .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
            .setConnPoolPolicy(PoolReusePolicy.LIFO)
            .setDefaultConnectionConfig(
                ConnectionConfig.custom()
                    .setConnectTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT.get(settings).getMillis()))
                    .setSocketTimeout(Timeout.ofMilliseconds(SOCKET_TIMEOUT.get(settings).getMillis()))
                    .setTimeToLive(TimeValue.ofMinutes(DEFAULT_TTL_MINUTES))
                    .build()
            )
            .setDefaultTlsConfig(
                TlsConfig.custom()
                    .setVersionPolicy(HttpVersionPolicy.NEGOTIATE)
                    .setHandshakeTimeout(Timeout.ofMinutes(DEFAULT_SSL_HANDSHAKE_TIMEOUT_MINUTES))
                    .build()
            )
            .setMaxConnTotal(MAX_CONNECTIONS.get(settings))
            .setMaxConnPerRoute(MAX_CONNECTIONS.get(settings))
            .build();

        final CloseableHttpAsyncClient httpAsyncClient = HttpAsyncClients.custom().setConnectionManager(connectionManager).build();
        httpAsyncClient.start();
        return httpAsyncClient;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> set = new HashSet<>();
        set.add(CONNECT_TIMEOUT);
        set.add(SOCKET_TIMEOUT);
        set.add(MAX_CONNECTIONS);
        return set;
    }
}
