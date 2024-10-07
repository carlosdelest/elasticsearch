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

import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsSchedulingTaskExecutor;
import co.elastic.elasticsearch.metering.usagereports.UsageReportService;
import co.elastic.elasticsearch.metering.usagereports.publisher.HttpClientThreadFilter;
import co.elastic.elasticsearch.metering.usagereports.publisher.HttpMeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchShardSizeCollector;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.compress.CompressedXContent;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@SuppressForbidden(reason = "Uses an HTTP server for testing")
@ThreadLeakFilters(filters = { HttpClientThreadFilter.class })
public abstract class AbstractMeteringIntegTestCase extends AbstractStatelessIntegTestCase {
    protected static final TimeValue REPORT_PERIOD = TimeValue.timeValueSeconds(3);
    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    private final BlockingQueue<List<UsageRecord>> received = new LinkedBlockingQueue<>();
    private final String projectId = randomAlphaOfLength(8);
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
        List<UsageRecord> invalid = received.stream().flatMap(List::stream).filter(r -> r.id().contains(projectId) == false).toList();
        assertTrue("Expected usage record ids to contain project id, but got: " + invalid, invalid.isEmpty());
        return received;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return super.nodeSettings().put(ServerlessSharedSettings.PROJECT_ID.getKey(), projectId)
            .put(HttpMeteringUsageRecordPublisher.METERING_URL.getKey(), "http://localhost:" + server.getAddress().getPort())
            // speed things up a bit
            .put(UsageReportService.REPORT_PERIOD.getKey(), REPORT_PERIOD.toString())
            .put(SampledClusterMetricsSchedulingTaskExecutor.POLL_INTERVAL_SETTING.getKey(), "1s")
            .put(SearchShardSizeCollector.PUSH_INTERVAL_SETTING.getKey(), "500ms")
            .put("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .put("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .build();
    }

    protected String startMasterIndexAndIngestNode() {
        return internalCluster().startNode(
            settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.INGEST_ROLE)
        );
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

    protected void pollReceivedRecords(List<UsageRecord> usageRecords) {
        List<List<UsageRecord>> recordLists = new ArrayList<>();
        receivedMetrics().drainTo(recordLists);
        recordLists.stream().flatMap(List::stream).forEach(usageRecords::add);
    }

    protected static void createDataStreamAndTemplate(String dataStreamName, String mapping) throws IOException {
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(Collections.singletonList(dataStreamName))
                    .template(new Template(null, new CompressedXContent(mapping), null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
        ).actionGet();
        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
        ).actionGet();
    }

    protected void createDataStream(String indexName) throws IOException {
        String mapping = mappingWithTimestamp();
        createDataStreamAndTemplate(indexName, mapping);
    }

    protected static String emptyMapping() {
        return """
            {
                  "properties": {
                 }
            }""";
    }

    protected static String mappingWithTimestamp() {
        return """
            {
                  "properties": {
                    "@timestamp": {
                      "type": "date"
                    }
                 }
            }""";
    }
}
