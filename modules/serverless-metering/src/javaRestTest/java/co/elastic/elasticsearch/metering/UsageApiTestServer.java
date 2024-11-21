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

package co.elastic.elasticsearch.metering;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class UsageApiTestServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(UsageApiTestServer.class);
    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();
    private static HttpServer server;
    private final BlockingQueue<List<Map<?, ?>>> received = new LinkedBlockingQueue<>();

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @Override
    protected void after() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            received.add(toUsageRecordMaps(exchange.getRequestBody()));
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private List<Map<?, ?>> toUsageRecordMaps(InputStream input) throws IOException {
        XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, input);
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        return XContentParserUtils.parseList(parser, XContentParser::map);
    }

    BlockingQueue<List<Map<?, ?>>> getReceivedRecords() {
        return received;
    }

    InetSocketAddress getAddress() {
        return server.getAddress();
    }
}
