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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;

public class UniversalIamTestServer extends ExternalResource {
    record AuthenticateProjectResponse(String type, String apiKeyId, String organizationId, String... applicationRoles) {
        XContentBuilder toXContent() throws IOException {
            final XContentBuilder xcb = XContentBuilder.builder(XContentType.JSON.xContent());
            xcb.startObject();
            xcb.field("type", type);
            xcb.field("api_key_id", apiKeyId);
            xcb.field("organization_id", organizationId);
            xcb.field("application_roles", applicationRoles);
            xcb.endObject();
            return xcb;
        }
    }

    private static final Logger logger = LogManager.getLogger(UniversalIamTestServer.class);

    @SuppressForbidden(reason = "HTTP server for testing")
    private static HttpServer server;

    private volatile AuthenticateProjectResponse response;

    @SuppressForbidden(reason = "HTTP server for testing")
    @Override
    protected void before() throws Throwable {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/uiam/api/v1/authentication/_authenticate-project", this::handle);
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

    void setResponse(AuthenticateProjectResponse response) {
        this.response = response;
    }

    @SuppressForbidden(reason = "HTTP server for testing")
    private void handle(HttpExchange exchange) throws IOException {
        logger.info("Received request: {}", exchange.getRequestURI());
        assertThat(response, is(not(nullValue())));
        try (exchange) {
            final BytesReference responseBytes = BytesReference.bytes(response.toXContent());
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, responseBytes.length());
            responseBytes.writeTo(exchange.getResponseBody());
        }
    }

    @SuppressForbidden(reason = "HTTP server for testing")
    InetSocketAddress getAddress() {
        return server.getAddress();
    }
}
