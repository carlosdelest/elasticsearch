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

import co.elastic.elasticsearch.metering.MeteringFeatures;
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateSupplier;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.Optional;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.elasticsearch.cluster.ClusterState.EMPTY_STATE;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

public class ClusterStateSampledMetricsTimeCursorTests extends ESTestCase {

    private static class TestClusterStateSupplier implements ClusterStateSupplier {
        ClusterStateSupplier clusterStateProvider = Optional::empty;

        void setClusterStateProvider(ClusterStateSupplier clusterStateProvider) {
            this.clusterStateProvider = clusterStateProvider;
        }

        @Override
        public Optional<ClusterState> get() {
            return clusterStateProvider.get();
        }
    }

    private final TestClusterStateSupplier clusterStateProvider = new TestClusterStateSupplier();

    @Mock
    FeatureService featureService;
    @Mock
    Client client;

    ClusterStateSampledMetricsTimeCursor timeCursor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        timeCursor = new ClusterStateSampledMetricsTimeCursor(clusterStateProvider, Settings.EMPTY, featureService, client);
    }

    public void testGetLatestCommittedTimestampIfUninitialized() {
        assertThat(timeCursor.getLatestCommittedTimestamp(), isEmpty());
    }

    public void testGetLatestCommittedTimestamp() {
        clusterStateProvider.setClusterStateProvider(() -> Optional.of(EMPTY_STATE));
        assertThat(timeCursor.getLatestCommittedTimestamp(), isEmpty());

        SampledMetricsMetadata metadata = new SampledMetricsMetadata(Instant.now());
        ClusterState state = EMPTY_STATE.copyAndUpdate(b -> b.putCustom(SampledMetricsMetadata.TYPE, metadata));
        clusterStateProvider.setClusterStateProvider(() -> Optional.of(state));

        assertThat(timeCursor.getLatestCommittedTimestamp(), isPresentWith(metadata.getCommittedTimestamp()));
    }

    public void testGenerateSampleTimestampsIfUninitialized() {
        assertThat(timeCursor.generateSampleTimestamps(Instant.now(), TimeValue.ONE_MINUTE).hasNext(), is(false));
    }

    public void testGenerateSampleTimestamps() {
        clusterStateProvider.setClusterStateProvider(() -> Optional.of(EMPTY_STATE));

        Instant now = Instant.now().truncatedTo(MINUTES);
        assertThat(Iterators.toList(timeCursor.generateSampleTimestamps(now, TimeValue.ONE_MINUTE)), contains(now));

        Instant committedTimestamp = now.minus(3, MINUTES);
        SampledMetricsMetadata metadata = new SampledMetricsMetadata(committedTimestamp);
        clusterStateProvider.setClusterStateProvider(
            () -> Optional.of(ClusterState.builder(new ClusterName("test")).putCustom(SampledMetricsMetadata.TYPE, metadata).build())
        );

        assertThat(timeCursor.generateSampleTimestamps(committedTimestamp, TimeValue.ONE_MINUTE).hasNext(), is(false));

        assertThat(
            Iterators.toList(timeCursor.generateSampleTimestamps(now, TimeValue.ONE_MINUTE)),
            contains(now, now.minus(1, MINUTES), now.minus(2, MINUTES))
        );
    }

    public void testCommitUpToIfUnavailable() {
        clusterStateProvider.setClusterStateProvider(() -> Optional.of(EMPTY_STATE));
        when(featureService.clusterHasFeature(Mockito.any(), Mockito.eq(MeteringFeatures.SAMPLED_METRICS_METADATA))).thenReturn(false);

        assertThat(timeCursor.commitUpTo(Instant.now()), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testCommitUpTo() {
        clusterStateProvider.setClusterStateProvider(() -> Optional.of(EMPTY_STATE));
        when(featureService.clusterHasFeature(Mockito.any(), Mockito.eq(MeteringFeatures.SAMPLED_METRICS_METADATA))).thenReturn(true);
        Instant timestamp = Instant.now();

        Mockito.doAnswer(invocation -> {
            // complete the listener
            ((ActionListener<ActionResponse.Empty>) invocation.getArguments()[2]).onResponse(ActionResponse.Empty.INSTANCE);
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());

        assertThat(timeCursor.commitUpTo(timestamp), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testCommitUpToFails() {
        when(featureService.clusterHasFeature(Mockito.any(), Mockito.eq(MeteringFeatures.SAMPLED_METRICS_METADATA)))
            .thenReturn(true);
        Instant timestamp = Instant.now();

        Mockito.doAnswer(invocation -> {
            // complete the listener
            ((ActionListener<ActionResponse.Empty>) invocation.getArguments()[2]).onFailure(new ElasticsearchException("timeout"));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());

        assertThat(timeCursor.commitUpTo(timestamp), is(false));
    }
}
