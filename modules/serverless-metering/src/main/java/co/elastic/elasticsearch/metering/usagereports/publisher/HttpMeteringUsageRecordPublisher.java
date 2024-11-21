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

package co.elastic.elasticsearch.metering.usagereports.publisher;

import org.elasticsearch.Build;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.AccessController;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

public class HttpMeteringUsageRecordPublisher implements MeteringUsageRecordPublisher {
    private static final Logger log = LogManager.getLogger(HttpMeteringUsageRecordPublisher.class);

    private static final String USER_AGENT = "elasticsearch/metering/" + Build.current().version();
    private static final TimeValue DEFAULT_REQUEST_TIMEOUT = TimeValue.timeValueSeconds(30);

    static final String USAGE_API_REQUESTS_TOTAL = "es.metering.usage_api.request.total";
    static final String USAGE_API_ERRORS_TOTAL = "es.metering.usage_api.error.total"; // (include http status in the attributes)
    static final String USAGE_API_REQUESTS_TIME = "es.metering.usage_api.request.time";
    static final String USAGE_API_REQUESTS_SIZE = "es.metering.usage_api.request.size";
    static final String STATUS_CODE_KEY = "es_metering_status_code";

    public static final Setting<URI> METERING_URL = new Setting<>("metering.url", "https://usage-api.usage-api/api/v1/usage", s -> {
        try {
            return new URI(s);
        } catch (URISyntaxException e) {
            throw new SettingsException("Cannot parse metering.url setting as a URL", e);
        }
    }, Setting.Property.NodeScope);

    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting("metering.batch_size", 100, Setting.Property.NodeScope);

    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.timeSetting(
        "metering.request_timeout",
        DEFAULT_REQUEST_TIMEOUT,
        Setting.Property.NodeScope
    );

    private static final TrustManager TRUST_EVERYTHING = new X509ExtendedTrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
    };

    private final URI meteringUri;
    private final Duration requestTimeout;
    private final int batchSize;
    private final HttpClient client;
    private final LongCounter requestsTotalCounter;
    private final LongCounter requestsErrorCounter;
    private final LongHistogram requestsSize;
    private final LongHistogram requestsTime;

    public HttpMeteringUsageRecordPublisher(Settings settings, MeterRegistry meterRegistry) {
        this.meteringUri = METERING_URL.get(settings);
        this.batchSize = BATCH_SIZE.get(settings);
        this.requestTimeout = Duration.ofMillis(REQUEST_TIMEOUT.get(settings).millis());

        SSLContext context;
        try {
            // don't check the SSL cert for now
            // TODO ES-6505
            context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[] { TRUST_EVERYTHING }, new SecureRandom());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            // SSL error that shouldn't happen
            assert false : e;
            throw new RuntimeException(e);
        }

        client = HttpClient.newBuilder().sslContext(context).followRedirects(HttpClient.Redirect.NORMAL).build();

        this.requestsTotalCounter = meterRegistry.registerLongCounter(
            USAGE_API_REQUESTS_TOTAL,
            "The total number of REST request to usage-api",
            "unit"
        );
        this.requestsErrorCounter = meterRegistry.registerLongCounter(
            USAGE_API_ERRORS_TOTAL,
            "The total number of unsuccessful REST request to usage-api",
            "unit"
        );
        this.requestsSize = meterRegistry.registerLongHistogram(USAGE_API_REQUESTS_SIZE, "Size of REST request to usage-api", "bytes");
        this.requestsTime = meterRegistry.registerLongHistogram(
            USAGE_API_REQUESTS_TIME,
            "Round-trip time of REST request to usage-api",
            "ms"
        );
    }

    /**
     * Publish usage records to the usage API.
     *
     * <p>Retries shall be handled by the caller, as soon as this encounters an error it will return false.
     * Note, requests are sent in batches, so some requests may have been sent successfully before an error is encountered.
     */
    @Override
    public boolean sendRecords(List<UsageRecord> records) {
        log.trace(() -> Strings.format("Sending records: %s", records));
        if (records.isEmpty()) {
            return true;
        }
        int successCount = 0;
        List<List<UsageRecord>> batches = CollectionUtils.eagerPartition(records, batchSize);
        for (int i = 0; i < batches.size(); i++) {
            requestsTotalCounter.increment();
            Instant startedAt = Instant.now();
            try {
                var batch = batches.get(i);
                HttpRequest request = createRequest(batch);

                var response = AccessController.doPrivileged(
                    (PrivilegedExceptionAction<HttpResponse<String>>) () -> client.send(request, HttpResponse.BodyHandlers.ofString())
                );
                var success = handleResponse(response, batch);
                if (success == false) {
                    if (batches.size() > 1) { // otherwise logs for current batch suffice
                        logFailedRecords(records.size(), successCount, batches.size(), i, startedAt, null);
                    }
                    return false;
                }
                successCount += batch.size();
            } catch (PrivilegedActionException e) {
                logFailedRecords(records.size(), successCount, batches.size(), i, startedAt, e.getCause());
                requestsErrorCounter.increment();
                return false;
            } catch (IOException | RuntimeException e) {
                logFailedRecords(records.size(), successCount, batches.size(), i, startedAt, e);
                requestsErrorCounter.increment();
                return false;
            } finally {
                Instant completedAt = Instant.now();
                requestsTime.record(startedAt.until(completedAt, ChronoUnit.MILLIS));
            }
        }
        return true;
    }

    private void logFailedRecords(
        int totalRecordsCount,
        int successCount,
        int batchesCount,
        int currentBatch,
        Instant startedAt,
        Throwable e
    ) {
        log.warn(
            "Failed to send {} records [of {} batches] to usage api [partial success: {} records in {} batches] after [{}]",
            totalRecordsCount - successCount,
            batchesCount - currentBatch,
            successCount,
            currentBatch,
            TimeValue.timeValueMillis(startedAt.until(Instant.now(), ChronoUnit.MILLIS)),
            e
        );
    }

    private boolean handleResponse(HttpResponse<?> response, List<UsageRecord> records) {
        int statusCode = response.statusCode();
        if (statusCode == 201) {
            // all ok
            return true;
        }
        switch (statusCode / 100) {
            case 2 -> {
                // some other success - not expecting this?
                log.info("Unexpected status code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
                return true;
            }
            case 4 -> {
                // problem with the request...?
                log.warn("Unexpected status code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
                requestsErrorCounter.incrementBy(1, Map.of(STATUS_CODE_KEY, response.statusCode()));
            }
            case 5 -> {
                // problem with the server
                log.warn("Received error code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
                requestsErrorCounter.incrementBy(1, Map.of(STATUS_CODE_KEY, response.statusCode()));
            }
            default -> {
                // another status code?
                log.error("Unexpected status code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
                requestsErrorCounter.incrementBy(1, Map.of(STATUS_CODE_KEY, response.statusCode()));
            }
        }
        return false; // request for batch failed
    }

    private HttpRequest createRequest(List<UsageRecord> records) throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(Map.of("creation_timestamp", Instant.now().toString()));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startArray();
        for (UsageRecord r : records) {
            r.toXContent(builder, params);
        }
        builder.endArray();

        BytesReference recordData = BytesReference.bytes(builder);
        requestsSize.record(recordData.length());

        return HttpRequest.newBuilder(meteringUri)
            .timeout(requestTimeout)
            .POST(HttpRequest.BodyPublishers.ofByteArray(recordData.array(), recordData.arrayOffset(), recordData.length()))
            .header("Content-Type", "application/json")
            .header("User-Agent", USER_AGENT)
            .build();
    }
}
