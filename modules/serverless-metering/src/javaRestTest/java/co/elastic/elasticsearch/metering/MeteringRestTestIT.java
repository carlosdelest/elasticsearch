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

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.test.cluster.serverless.DefaultServerlessLocalConfigProvider.node;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class MeteringRestTestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(MeteringRestTestIT.class);

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();
    private static HttpServer server;
    private static final BlockingQueue<List<Map<?, ?>>> received = new LinkedBlockingQueue<>();
    static int REPORT_PERIOD = 5;

    @BeforeClass
    public static void setupServer() throws IOException {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", MeteringRestTestIT::handle);
        server.start();
    }

    @Rule
    public ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .withNode(node("index2", "index"))// first node created by default
        .withNode(node("index3", "index"))
        .withNode(node("search2", "search"))// first node created by default
        .name("javaRestTest")
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.ml.enabled", "false")
        .setting("metering.project_id", "testProjectId")
        .setting("metering.url", "http://localhost:" + server.getAddress().getPort())
        .setting("metering.report_period", REPORT_PERIOD + "s") // speed things up a bit
        .build();

    @AfterClass
    public static void stopServer() {
        server.stop(0);
    }

    private static void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            received.add(toUsageRecordMaps(exchange.getRequestBody()));
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private static List<Map<?, ?>> toUsageRecordMaps(InputStream input) throws IOException {
        XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, input);
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        return XContentParserUtils.parseList(parser, XContentParser::map);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testMeteringRecordsCanBeDeduplicated() throws Exception {
        // This test asserts that a duplicated metrics (due to multiple replicas) can be deduplicated.
        // metrics can only be deduplicated if their id is the same (including the timestamp)
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .build();
        String indexName = "test_index_1";
        createIndex(indexName, settings);

        // ingest more docs so that each shard has some
        int numDocs = 99;
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"foo\": \"bar\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);

        ensureGreen(indexName);

        client().performRequest(new Request("POST", "/" + indexName + "/_flush"));

        logShardAllocationInformation(indexName);

        assertBusy(() -> {
            var usageRecords = getUsageRecords("shard-size");
            // there might be records from multiple metering.report_period. We are interested in the latest
            // because the replication might take time and also some nodes might be reporting with a delay

            Optional<Instant> timestamp = getLatestUsageTimestamp(usageRecords);
            if (timestamp.isPresent()) {
                List<Map<?, ?>> latestRecords = getRecordsForLatestTimestamp(usageRecords, timestamp);

                // there are 6 records. Each primary shard (3) has 2 replicas. This accounts to 6 replica shards.
                assertThat(latestRecords.size(), equalTo(6));
                Map<?, List<Map<?, ?>>> groupedById = latestRecords.stream().collect(groupingBy(m -> m.get("id")));
                // there are 3 primary shards, so should be 3 unique ids only
                assertThat(groupedById, equalTo(3));
                // each primary is duplicated (replicated) twice
                groupedById.values().forEach(l -> assertThat(l, hasSize(2)));
            }
        }, 30, TimeUnit.SECONDS);

    }

    private static List<Map<?, ?>> getRecordsForLatestTimestamp(List<Map<?, ?>> metric, Optional<Instant> timestamp) {
        return metric.stream().filter(m -> m.get("usage_timestamp").equals(timestamp.get().toString())).collect(Collectors.toList());
    }

    private static Optional<Instant> getLatestUsageTimestamp(List<Map<?, ?>> metric) {
        return metric.stream()
            .map(m -> m.get("usage_timestamp"))
            .map(t -> Instant.parse((String) t))
            .sorted(Comparator.reverseOrder())
            .findFirst();
    }

    private static void logShardAllocationInformation(String indexName) throws IOException {
        Response nodeNamesResponse = client().performRequest(new Request("GET", "/_cat/nodes?h=name"));
        String nodeNames = EntityUtils.toString(nodeNamesResponse.getEntity());
        for (int shardNumber = 0; shardNumber < 3; shardNumber++) {
            for (String nodeName : nodeNames.split("\n")) {
                for (boolean isPrimary : new boolean[] { true, false }) {
                    try {
                        Request get = new Request("GET", "/_cluster/allocation/explain");
                        get.setJsonEntity(Strings.format("""
                            {
                              "index": "%s",
                              "shard" : %d,
                              "primary": %b,
                              "current_node": "%s"
                            }
                            """, indexName, shardNumber, isPrimary, nodeName));
                        Response response = client().performRequest(get);
                        logger.info((isPrimary ? "primary " : "replica ") + shardNumber);
                        logger.info(
                            XContentHelper.convertToJson(
                                BytesReference.fromByteBuffer(ByteBuffer.wrap(EntityUtils.toByteArray(response.getEntity()))),
                                true
                            )
                        );
                    } catch (ResponseException e) {

                    }
                }
            }
        }
    }

    private List<Map<?, ?>> getUsageRecords(String prefix) {
        List<List<Map<?, ?>>> recordLists = new ArrayList<>();
        received.drainTo(recordLists);
        logger.info(recordLists);
        return recordLists.stream()
            .flatMap(List::stream)
            .filter(m -> ((String) m.get("id")).startsWith(prefix))
            .collect(Collectors.toList());
    }

}
