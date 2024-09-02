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

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.test.ESTestCase;
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

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
@ThreadLeakFilters(filters = { HttpClientThreadFilter.class })
public class HttpMeteringUsageRecordPublisherTests extends ESTestCase {

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    private HttpServer server;
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

        settings = Settings.builder().put(HttpMeteringUsageRecordPublisher.METERING_URL.getKey(), "http://localhost:" + port).build();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            assertThat(exchange.getRequestMethod(), equalTo("POST"));
            assertTrue(exchange.getRequestHeaders().containsKey(HttpHeaders.USER_AGENT));
            assertThat(exchange.getRequestHeaders().get(HttpHeaders.USER_AGENT), contains(startsWith("elasticsearch/metering")));

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
            new UsageMetrics("type", null, 1, null, null, null),
            new UsageSource("es-id", "instanceId", null)
        );

        var meterRegistry = new RecordingMeterRegistry();
        try (HttpMeteringUsageRecordPublisher reporter = new HttpMeteringUsageRecordPublisher(settings, meterRegistry)) {
            reporter.start();
            reporter.sendRecords(List.of(record));

            List<?> data = requests.poll(10, TimeUnit.SECONDS);
            assertNotNull("Request was not received in time", data);
            assertRecord(List.of(record), data);

            reporter.stop();
        }

        final var requests = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TOTAL);
        final var errors = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_ERRORS_TOTAL);
        final var sizes = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_SIZE);
        final var times = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TIME);

        assertThat(requests, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(errors, empty());
        assertThat(sizes, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
        assertThat(times, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
    }

    public void testReporterSendsLotsOfData() throws Exception {
        Settings testSettings = Settings.builder().put(settings).put(HttpMeteringUsageRecordPublisher.BATCH_SIZE.getKey(), 10).build();

        List<UsageRecord> records = IntStream.range(0, 25)
            .mapToObj(
                i -> new UsageRecord(
                    "id" + i,
                    Instant.now(),
                    new UsageMetrics("type", null, 1, null, null, null),
                    new UsageSource("es-id", "instanceId", null)
                )
            )
            .toList();

        var meterRegistry = new RecordingMeterRegistry();
        try (HttpMeteringUsageRecordPublisher reporter = new HttpMeteringUsageRecordPublisher(testSettings, meterRegistry)) {
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

        final var requests = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TOTAL);
        final var errors = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_ERRORS_TOTAL);
        final var sizes = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_SIZE);
        final var times = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TIME);

        assertThat(requests, contains(transformedMatch(Measurement::getLong, equalTo(3L))));
        assertThat(errors, empty());
        assertThat(sizes, everyItem(transformedMatch(Measurement::getLong, greaterThan(0L))));
        assertThat(times, everyItem(transformedMatch(Measurement::getLong, greaterThan(0L))));
    }

    public void testServerDown() {
        server.stop(0);     // bye bye server

        UsageRecord record = new UsageRecord(
            "id1",
            Instant.now(),
            new UsageMetrics("type", null, 1, null, null, null),
            new UsageSource("es-id", "instanceId", null)
        );

        var meterRegistry = new RecordingMeterRegistry();
        try (HttpMeteringUsageRecordPublisher reporter = new HttpMeteringUsageRecordPublisher(settings, meterRegistry)) {
            reporter.start();

            expectThrows(ConnectException.class, () -> reporter.sendRecords(List.of(record)));
        }

        final var requests = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TOTAL);
        final var errors = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_ERRORS_TOTAL);
        final var sizes = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_SIZE);
        final var times = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TIME);

        assertThat(requests, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(errors, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(sizes, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
        assertThat(times, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
    }

    public void testServerFailure() throws IOException, InterruptedException {
        nextRequestStatusCode = 500;    // server is wrong

        UsageRecord record = new UsageRecord(
            "id1",
            Instant.now(),
            new UsageMetrics("type", null, 1, null, null, null),
            new UsageSource("es-id", "instanceId", null)
        );

        var meterRegistry = new RecordingMeterRegistry();
        try (HttpMeteringUsageRecordPublisher reporter = new HttpMeteringUsageRecordPublisher(settings, meterRegistry)) {
            reporter.start();
            reporter.sendRecords(List.of(record));
        }

        final var requests = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TOTAL);
        final var errors = getCounter(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_ERRORS_TOTAL);
        final var sizes = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_SIZE);
        final var times = getHistogram(meterRegistry.getRecorder(), HttpMeteringUsageRecordPublisher.USAGE_API_REQUESTS_TIME);

        assertThat(requests, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(errors, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(sizes, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
        assertThat(times, contains(transformedMatch(Measurement::getLong, greaterThan(0L))));
    }

    @After
    public void cleanup() {
        server.stop(0);

        // drain all requests
        if (requests.isEmpty() == false) {
            fail("Requests were unprocessed: " + requests);
        }
    }

    private static List<Measurement> getCounter(MetricRecorder<Instrument> recorder, String metricName) {
        return Measurement.combine(recorder.getMeasurements(InstrumentType.LONG_COUNTER, metricName));
    }

    private static List<Measurement> getHistogram(MetricRecorder<Instrument> recorder, String metricName) {
        return recorder.getMeasurements(InstrumentType.LONG_HISTOGRAM, metricName);
    }
}
