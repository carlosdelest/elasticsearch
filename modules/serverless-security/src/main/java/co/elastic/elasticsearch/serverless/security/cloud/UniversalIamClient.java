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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static co.elastic.elasticsearch.serverless.security.ServerlessSecurityPlugin.UNIVERSAL_IAM_SERVICE_URL_SETTING;

public class UniversalIamClient implements Closeable {

    private static final Logger logger = LogManager.getLogger(UniversalIamClient.class);
    private static final String BASE_UIAM_PATH = "/uiam/api/v1";
    private static final String AUTHENTICATE_PROJECT_ENDPOINT = BASE_UIAM_PATH + "/authentication/_authenticate-project";

    private final URI authenticateProjectUri;
    private final CloseableHttpAsyncClient httpClient;

    public UniversalIamClient(Settings settings) {
        this.authenticateProjectUri = UNIVERSAL_IAM_SERVICE_URL_SETTING.get(settings).resolve(AUTHENTICATE_PROJECT_ENDPOINT);
        this.httpClient = createHttpClient();
    }

    /**
     * Sends the authentication request to the universal IAM service.
     * <p>
     * The authentication request is expected to return a 200 response containing the authenticated user information,
     * or a 401 error if the authentication fails. Every other response is considered an internal error.
     */
    public void authenticateProject(CloudApiKeyAuthenticationRequest request, ActionListener<CloudApiKeyAuthenticationResponse> listener) {
        final HttpGet httpGet = toHttpGet(authenticateProjectUri, request);

        logger.debug("Authenticating against universal IAM service [{}]", httpGet.getURI());

        httpClient.execute(httpGet, new FutureCallback<>() {
            @Override
            public void completed(final HttpResponse result) {
                logger.debug("cloud API key authentication request against universal IAM service [{}] succeeded", httpGet.getURI());
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

    private void handleResponse(HttpResponse httpResponse, ActionListener<CloudApiKeyAuthenticationResponse> listener) {
        final HttpEntity entity = httpResponse.getEntity();
        final StatusLine statusLine = httpResponse.getStatusLine();
        if (false == RestStatus.isSuccessful(statusLine.getStatusCode())) {
            final String responseContent = readResponseContentAsString(entity);
            // TODO better failure handling -- consider parsing error details (if available) and tailoring message
            if (statusLine.getStatusCode() == RestStatus.UNAUTHORIZED.getStatus()) {
                logger.debug(
                    "Cloud API key authentication request failed with status [{}] and response [{}]",
                    statusLine.getStatusCode(),
                    responseContent
                );
                listener.onFailure(new ElasticsearchSecurityException("Cloud API key authentication failed."));
            } else {
                // everything else is considered as unexpected (i.e. internal server error)
                logger.error(
                    "Cloud API key authentication request failed with unexpected status [{}] and response [{}]",
                    statusLine.getStatusCode(),
                    responseContent
                );
                listener.onFailure(new IllegalStateException("Received unexpected response while authenticating cloud API key"));
            }
            return;
        }

        final Header contentTypeHeader = entity.getContentType();
        final String contentTypeValue = contentTypeHeader == null ? null : ContentType.parse(contentTypeHeader.getValue()).getMimeType();
        if (false == ContentType.APPLICATION_JSON.getMimeType().equals(contentTypeValue)) {
            listener.onFailure(
                new IllegalStateException(
                    "unable to parse response. Content type was expected to be [application/json] but was [" + contentTypeValue + "]"
                )
            );
            return;
        }

        try (InputStream inputStream = entity.getContent()) {
            listener.onResponse(CloudApiKeyAuthenticationResponse.parse(inputStream));
        } catch (Exception e) {
            listener.onFailure(new IllegalStateException("Failed to parse the response from universal IAM service", e));
        }
    }

    private static String readResponseContentAsString(HttpEntity entity) {
        if (entity == null) {
            return "";
        }
        try {
            return EntityUtils.toString(entity, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.debug("Failed to read response content", e);
            return "";
        }
    }

    protected XContentParser createJsonParser(InputStream stream) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, stream);
    }

    private static HttpGet toHttpGet(URI baseUri, CloudApiKeyAuthenticationRequest request) {
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
        final HttpGet httpGet = new HttpGet(fullUri);
        httpGet.addHeader(HttpHeaders.AUTHORIZATION, ApiKeyService.withApiKeyPrefix(cloudApiKey.cloudApiKeyCredentials().toString()));
        return httpGet;
    }

    // TODO move to httpclient5 and properly configure the client (TLS, timeouts, etc)
    private static CloseableHttpAsyncClient createHttpClient() {
        final CloseableHttpAsyncClient httpAsyncClient = HttpAsyncClients.createDefault();
        httpAsyncClient.start();
        return httpAsyncClient;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
