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

import co.elastic.elasticsearch.metering.reports.HttpClientThreadFilter;
import co.elastic.elasticsearch.metering.reports.MeteringReporter;
import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@SuppressForbidden(reason = "Uses an HTTP server for testing")
@ThreadLeakFilters(filters = { HttpClientThreadFilter.class })
public abstract class AbstractMeteringIntegTestCase extends AbstractStatelessIntegTestCase {

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    private final BlockingQueue<List<UsageRecord>> received = new LinkedBlockingQueue<>();
    private HttpServer server;

    @Before
    public void setupServer() throws IOException {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                List<UsageRecord> usageRecordMaps = toUsageRecordMaps(exchange.getRequestBody());
                received.add(usageRecordMaps);
            } catch (RuntimeException e) {
                logger.warn("failed to parse request", e);
            }
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private static List<UsageRecord> toUsageRecordMaps(InputStream input) throws IOException {
        XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, input);
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        return XContentParserUtils.parseList(parser, UsageRecord::fromXContent);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var list = new ArrayList<Class<? extends Plugin>>();
        list.addAll(super.nodePlugins());
        list.add(MeteringPlugin.class);
        list.add(MockTransportService.TestPlugin.class);
        return list;
    }

    protected BlockingQueue<List<UsageRecord>> receivedMetrics() {
        return received;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return super.nodeSettings().put(ServerlessSharedSettings.PROJECT_ID.getKey(), "testProjectId")
            .put(MeteringReporter.METERING_URL.getKey(), "http://localhost:" + server.getAddress().getPort())
            .put(MeteringService.REPORT_PERIOD.getKey(), TimeValue.timeValueSeconds(5)) // speed things up a bit
            .put("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .put("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .build();
    }

    protected String startMasterIndexAndIngestNode() {
        return internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.INGEST_ROLE)
        );
    }

    @Override
    protected Settings.Builder settingsForRoles(DiscoveryNodeRole... roles) {
        return super.settingsForRoles(roles).put(ServerlessSharedSettings.PROJECT_ID.getKey(), "testProjectId");
    }

    @After
    public void stopServer() throws Exception {
        // explicitly clean up the cluster here
        // the cluster needs to be stopped BEFORE the http server is stopped
        cleanUpCluster();

        server.stop(1);
    }

    protected boolean hasReceivedRecords(String prefix) {
        return receivedMetrics().stream().flatMap(List::stream).anyMatch(m -> m.id().startsWith(prefix));
    }

    protected UsageRecord pollReceivedRecordsAndGetFirst(String prefix) {
        List<List<UsageRecord>> recordLists = new ArrayList<>();
        receivedMetrics().drainTo(recordLists);

        return recordLists.stream().flatMap(List::stream).filter(m -> m.id().startsWith(prefix)).findFirst().get();
    }
}
