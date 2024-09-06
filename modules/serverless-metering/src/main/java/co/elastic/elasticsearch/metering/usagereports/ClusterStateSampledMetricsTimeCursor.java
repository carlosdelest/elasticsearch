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

package co.elastic.elasticsearch.metering.usagereports;

import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;
import co.elastic.elasticsearch.metering.usagereports.action.UpdateSampledMetricsMetadataAction;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Instant;
import java.util.Optional;

import static co.elastic.elasticsearch.metering.MeteringFeatures.SAMPLED_METRICS_METADATA;
import static co.elastic.elasticsearch.metering.usagereports.UsageReportService.REPORT_PERIOD;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class ClusterStateSampledMetricsTimeCursor implements SampledMetricsTimeCursor {

    private static final Logger logger = LogManager.getLogger(ClusterStateSampledMetricsTimeCursor.class);
    private static final TimeValue MINIMUM_TRANSPORT_ACTION_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final ClusterService clusterService;
    private final FeatureService featureService;

    private final Client client;
    private final TimeValue transportActionTimeout;

    // while listener is NOT null, initialization is pending
    private volatile ClusterStateListener initializationListener;

    ClusterStateSampledMetricsTimeCursor(ClusterService clusterService, FeatureService featureService, Client client) {
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.client = client;
        var reportPeriod = REPORT_PERIOD.get(clusterService.getSettings());
        // taking as long as the report period will back-pressure the next run (and generate a warning)
        this.transportActionTimeout = reportPeriod.compareTo(MINIMUM_TRANSPORT_ACTION_TIMEOUT) > 0
            ? reportPeriod
            : MINIMUM_TRANSPORT_ACTION_TIMEOUT;
        ClusterStateListener initializationListener = this::init;
        clusterService.addListener(initializationListener);
        this.initializationListener = initializationListener;
    }

    private boolean isInitialized() {
        return initializationListener == null; // successfully initialized once the listener is removed
    }

    protected void init(ClusterChangedEvent clusterChangedEvent) {
        if (clusterChangedEvent.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
            ClusterStateListener listener = initializationListener;
            if (listener != null) {
                initializationListener = null;
                clusterService.removeListener(listener);
            }
        }
    }

    @Override
    public Timestamps generateSampleTimestamps(Instant current, TimeValue period) {
        if (isInitialized()) {
            return getLatestCommittedTimestamp().map(x -> SampledMetricsTimeCursor.generateSampleTimestamps(current, x, period))
                .orElseGet(() -> Timestamps.single(current));

        }
        return Timestamps.EMPTY;
    }

    @Override
    public Optional<Instant> getLatestCommittedTimestamp() {
        if (isInitialized()) {
            var clusterState = clusterService.state();
            var metadata = SampledMetricsMetadata.getFromClusterState(clusterState);
            if (metadata != null) {
                return Optional.ofNullable(metadata.getCommittedTimestamp());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean commitUpTo(Instant timestamp) {
        if (featureService.clusterHasFeature(clusterService.state(), SAMPLED_METRICS_METADATA) == false) {
            return true; // not yet supported, treat as success
        }
        logger.debug("Updating committed timestamp to [{}]", timestamp);
        PlainActionFuture<ActionResponse.Empty> listener = new PlainActionFuture<>();
        client.execute(
            UpdateSampledMetricsMetadataAction.INSTANCE,
            new UpdateSampledMetricsMetadataAction.Request(transportActionTimeout, new SampledMetricsMetadata(timestamp)),
            listener
        );
        try {
            listener.actionGet();
            return true;
        } catch (RuntimeException e) {
            logger.warn("Master request to update samples timestamp failed", e);
            return false;
        }
    }
}
