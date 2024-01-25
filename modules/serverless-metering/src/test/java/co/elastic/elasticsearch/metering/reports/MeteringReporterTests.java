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

package co.elastic.elasticsearch.metering.reports;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.HttpStatus;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
@ThreadLeakFilters(filters = { HttpClientThreadFilter.class })
public class MeteringReporterTests extends ESTestCase {

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    private HttpServer server;
    private TestThreadPool threadPool;
    private Settings settings;

    private final BlockingQueue<List<?>> requests = new LinkedBlockingQueue<>();
    private volatile int nextRequestStatusCode = HttpStatus.SC_CREATED;

    @Before
    public void setupServer() throws IOException {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        int port = server.getAddress().getPort();

        server.createContext("/", this::handle);
        server.start();

        threadPool = new TestThreadPool("meteringReporterTests");

        settings = Settings.builder().put(MeteringReporter.METERING_URL.getKey(), "http://localhost:" + port).build();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            assertThat(exchange.getRequestMethod(), equalTo("POST"));

            if (nextRequestStatusCode == HttpStatus.SC_CREATED) {
                var map = toUsageRecordMaps(exchange.getRequestBody());
                requests.add(map);
            }

            exchange.sendResponseHeaders(nextRequestStatusCode, 0);
        }
    }

    private static List<?> toUsageRecordMaps(List<UsageRecord> records) throws IOException {
        var builder = XCONTENT.getContentBuilder().startArray();
        for (UsageRecord r : records) {
            r.toXContent(builder, EMPTY_PARAMS);
        }
        builder.endArray();
        builder.flush();
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(((ByteArrayOutputStream) builder.getOutputStream()).toByteArray());
        return toUsageRecordMaps(bytesIn);
    }

    private static List<?> toUsageRecordMaps(InputStream input) throws IOException {
        return XCONTENT.XContent()
            .createParser(XContentParserConfiguration.EMPTY.withFiltering(Set.of(), Set.of("creation_timestamp"), false), input)
            .list();
    }

    private static void assertRecord(List<UsageRecord> expected, List<?> data) throws IOException {
        assertThat(data, contains(toUsageRecordMaps(expected).toArray()));
    }

    public void testReporterSendsData() throws Exception {
        UsageRecord record = new UsageRecord(
            "id1",
            Instant.now(),
            new UsageMetrics("type", null, 1, null, null, null, null),
            new UsageSource("es-id", "instanceId", null)
        );

        try (MeteringReporter reporter = new MeteringReporter(settings, threadPool)) {
            reporter.start();
            reporter.sendRecords(List.of(record));

            List<?> data = requests.poll(10, TimeUnit.SECONDS);
            assertNotNull("Request was not received in time", data);
            assertRecord(List.of(record), data);

            reporter.stop();
        }
    }

    public void testReporterSendsLotsOfData() throws Exception {
        Settings testSettings = Settings.builder().put(settings).put(MeteringReporter.BATCH_SIZE.getKey(), 10).build();

        List<UsageRecord> records = IntStream.range(0, 25)
            .mapToObj(
                i -> new UsageRecord(
                    "id" + i,
                    Instant.now(),
                    new UsageMetrics("type", null, 1, null, null, null, null),
                    new UsageSource("es-id", "instanceId", null)
                )
            )
            .toList();

        try (MeteringReporter reporter = new MeteringReporter(testSettings, threadPool)) {
            reporter.start();
            reporter.sendRecords(records);

            List<Object> received = new ArrayList<>();
            waitUntil(() -> {
                try {
                    var polled = requests.poll(10, TimeUnit.SECONDS);
                    assertNotNull("Request was not received in time", polled);
                    received.addAll(polled);
                    return received.size() == records.size();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            assertRecord(records, received);

            reporter.stop();
        }
    }

    public void testServerDown() {
        server.stop(0);     // bye bye server

        UsageRecord record = new UsageRecord(
            "id1",
            Instant.now(),
            new UsageMetrics("type", null, 1, null, null, null, null),
            new UsageSource("es-id", "instanceId", null)
        );

        try (MeteringReporter reporter = new MeteringReporter(settings, threadPool)) {
            reporter.start();

            var e = expectThrows(AssertionError.class, () -> reporter.sendRecords(List.of(record)));

            assertThat(ExceptionsHelper.unwrap(e, ConnectException.class), instanceOf(ConnectException.class));
        }
    }

    public void testServerFailure() {
        nextRequestStatusCode = 500;    // server is wrong

        UsageRecord record = new UsageRecord(
            "id1",
            Instant.now(),
            new UsageMetrics("type", null, 1, null, null, null, null),
            new UsageSource("es-id", "instanceId", null)
        );

        try (MeteringReporter reporter = new MeteringReporter(settings, threadPool)) {
            reporter.start();

            // TODO: just log for now, needs some retries
            reporter.sendRecords(List.of(record));
        }
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        server.stop(0);

        // drain all requests
        if (requests.isEmpty() == false) {
            fail("Requests were unprocessed: " + requests);
        }
    }
}
