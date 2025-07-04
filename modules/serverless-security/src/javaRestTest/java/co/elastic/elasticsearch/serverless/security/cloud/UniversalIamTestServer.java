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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

public class UniversalIamTestServer extends ExternalResource {

    private static final Logger logger = LogManager.getLogger(UniversalIamTestServer.class);

    @SuppressForbidden(reason = "HTTP server for testing")
    private static HttpServer server;

    private volatile UniversalIamResponse response;

    @SuppressForbidden(reason = "HTTP server for testing")
    @Override
    protected void before() throws Throwable {
        server = configureHttpServer();
        server.start();
    }

    @SuppressForbidden(reason = "HTTP server for testing")
    @Override
    protected void after() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    void setResponse(UniversalIamResponse response) {
        this.response = response;
    }

    @SuppressForbidden(reason = "HTTP server for testing")
    private void handle(HttpExchange exchange) throws IOException {
        logger.info("Received request: {}", exchange.getRequestURI());
        assertThat(response, is(not(nullValue())));
        try (exchange) {
            final BytesReference responseBytes = response.toBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(response.responseCode(), responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());
        }
    }

    @SuppressForbidden(reason = "HTTP server for testing")
    InetSocketAddress getAddress() {
        return server.getAddress();
    }

    @SuppressForbidden(reason = "use http server")
    HttpServer configureHttpServer() throws IOException {
        HttpsServer server = HttpsServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/uiam/api/v1/authentication/_authenticate-project", this::handle);

        SSLContext sslContext;
        try (InputStream keystoreInput = getClass().getClassLoader().getResourceAsStream("uiam/unified-keystore.jks")) {
            char[] password = "changeit".toCharArray();
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(keystoreInput, password);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, password);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ks);

            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        server.setHttpsConfigurator(new CustomHttpsConfigurator(sslContext));

        return server;
    }

    @SuppressForbidden(reason = "use http server")
    private static final class CustomHttpsConfigurator extends HttpsConfigurator {

        CustomHttpsConfigurator(SSLContext sslContext) {
            super(sslContext);
        }

        @Override
        public void configure(HttpsParameters params) {
            SSLParameters sslParams = this.getSSLContext().getDefaultSSLParameters();
            sslParams.setCipherSuites(XPackSettings.DEFAULT_CIPHERS.toArray(Strings.EMPTY_ARRAY));
            sslParams.setProtocols(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS.toArray(Strings.EMPTY_ARRAY));
            params.setSSLParameters(sslParams);
        }
    }

    interface UniversalIamResponse {

        int responseCode();

        BytesReference toBytes() throws IOException;

    }

    record SuccessfulAuthenticateProjectResponse(
        String type,
        String apiKeyId,
        String apiKeyDescription,
        String organizationId,
        String... applicationRoles
    ) implements UniversalIamResponse {

        @Override
        public int responseCode() {
            return 200;
        }

        @Override
        public BytesReference toBytes() throws IOException {
            final XContentBuilder xcb = XContentBuilder.builder(XContentType.JSON.xContent());
            xcb.startObject();
            xcb.field("type", type);
            xcb.field("api_key_id", apiKeyId);
            xcb.field("organization_id", organizationId);
            xcb.field("application_roles", applicationRoles);
            if (apiKeyDescription != null) {
                xcb.field("api_key_description", apiKeyDescription);
            }
            xcb.endObject();
            return BytesReference.bytes(xcb);
        }

    }

    record FailedAuthenticateProjectResponse(int responseCode, String error) implements UniversalIamResponse {

        @Override
        public BytesReference toBytes() throws IOException {
            final XContentBuilder xcb = XContentBuilder.builder(XContentType.JSON.xContent());
            xcb.startObject();
            xcb.field("error", error);
            xcb.endObject();
            return BytesReference.bytes(xcb);
        }
    }

}
