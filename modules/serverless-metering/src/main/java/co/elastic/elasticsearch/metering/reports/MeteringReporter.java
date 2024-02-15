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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

public class MeteringReporter extends AbstractLifecycleComponent {

    private static final Logger log = LogManager.getLogger(MeteringReporter.class);

    public static final Setting<URI> METERING_URL = new Setting<>("metering.url", "https://usage-api.elastic-system/api/v1/usage", s -> {
        try {
            return new URI(s);
        } catch (URISyntaxException e) {
            throw new SettingsException("Cannot parse metering.url setting as a URL", e);
        }
    }, Setting.Property.NodeScope);

    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting("metering.batch_size", 100, Setting.Property.NodeScope);

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

    private final Settings settings;
    private final int batchSize;
    private final Scheduler scheduler;
    private final HttpClient client;

    public MeteringReporter(Settings settings, Scheduler scheduler) {
        this.settings = settings;
        this.batchSize = BATCH_SIZE.get(settings);
        this.scheduler = scheduler;

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
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    public void sendRecords(List<UsageRecord> records) {
        log.trace(() -> Strings.format("Sending records: %s", records));
        if (records.isEmpty()) return;

        List<List<UsageRecord>> batches = CollectionUtils.eagerPartition(records, batchSize);
        for (List<UsageRecord> batch : batches) {
            try {
                HttpRequest request = createRequest(batch);

                var response = AccessController.doPrivileged(
                    (PrivilegedExceptionAction<HttpResponse<String>>) () -> client.send(request, HttpResponse.BodyHandlers.ofString())
                );
                handleResponse(response, batch);
            } catch (IOException | PrivilegedActionException e) {
                // TODO: ES-6462 remove assert, log the record info, and retry
                assert false : e;
                log.error("Could not send {} records to billing service", batch.size(), e);
            }
        }
    }

    private static void handleResponse(HttpResponse<?> response, List<UsageRecord> records) {
        int statusCode = response.statusCode();
        if (statusCode == 201) {
            // all ok
            return;
        }
        switch (statusCode / 100) {
            case 2 -> {
                // some other success - not expecting this?
                log.info("Unexpected status code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
            }
            case 4 -> {
                // problem with the request...?
                log.warn("Unexpected status code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
            }
            case 5 -> {
                // problem with the server
                log.warn("Received error code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
            }
            default -> {
                // another status code?
                log.error("Unexpected status code {} sending {} records [{}]", response.statusCode(), records.size(), response.body());
            }
        }
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

        return HttpRequest.newBuilder(METERING_URL.get(settings))
            .POST(HttpRequest.BodyPublishers.ofByteArray(recordData.array(), recordData.arrayOffset(), recordData.length()))
            .header("Content-Type", "application/json")
            .build();
    }

    @Override
    protected void doClose() throws IOException {}
}
