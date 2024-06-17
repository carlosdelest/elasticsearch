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

import co.elastic.elasticsearch.metering.action.SampledMetricsMetadata;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.elasticsearch.cluster.ClusterState.EMPTY_STATE;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ClusterStateSampledMetricsTimeCursorTests extends ESTestCase {

    @Mock
    ClusterService clusterService;
    @Mock
    FeatureService featureService;
    @Mock
    Client client;

    ClusterStateSampledMetricsTimeCursor timeCursor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        timeCursor = new ClusterStateSampledMetricsTimeCursor(clusterService, featureService, client);
    }

    private void initCursor() {
        ArgumentCaptor<ClusterStateListener> captor = ArgumentCaptor.captor();
        Mockito.verify(clusterService).addListener(captor.capture());

        Mockito.when(clusterService.state()).thenReturn(EMPTY_STATE);

        captor.getValue().clusterChanged(new ClusterChangedEvent("test", EMPTY_STATE, EMPTY_STATE));
        Mockito.verify(clusterService).removeListener(captor.getValue());
    }

    public void testGetLatestCommittedTimestampIfUninitialized() {
        assertThat(timeCursor.getLatestCommittedTimestamp(), isEmpty());
    }

    public void testGetLatestCommittedTimestamp() {
        initCursor();
        assertThat(timeCursor.getLatestCommittedTimestamp(), isEmpty());

        SampledMetricsMetadata metadata = new SampledMetricsMetadata(Instant.now());
        ClusterState state = EMPTY_STATE.copyAndUpdate(b -> b.putCustom(SampledMetricsMetadata.TYPE, metadata));
        Mockito.when(clusterService.state()).thenReturn(state);

        assertThat(timeCursor.getLatestCommittedTimestamp(), isPresentWith(metadata.getCommittedTimestamp()));
    }

    public void testGenerateSampleTimestampsIfUninitialized() {
        assertThat(timeCursor.generateSampleTimestamps(Instant.now(), TimeValue.ONE_MINUTE, 10), is(empty()));
    }

    public void testGenerateSampleTimestamps() {
        initCursor();

        Instant now = Instant.now().truncatedTo(MINUTES);
        assertThat(timeCursor.generateSampleTimestamps(now, TimeValue.ONE_MINUTE, 10), contains(now));

        Instant committedTimestamp = now.minus(3, MINUTES);
        SampledMetricsMetadata metadata = new SampledMetricsMetadata(committedTimestamp);
        Mockito.when(clusterService.state())
            .thenReturn(ClusterState.builder(new ClusterName("test")).putCustom(SampledMetricsMetadata.TYPE, metadata).build());

        assertThat(timeCursor.generateSampleTimestamps(committedTimestamp, TimeValue.ONE_MINUTE, 10), is(empty()));

        assertThat(
            timeCursor.generateSampleTimestamps(now, TimeValue.ONE_MINUTE, 10),
            contains(now, now.minus(1, MINUTES), now.minus(2, MINUTES))
        );

        assertThat(timeCursor.generateSampleTimestamps(now, TimeValue.ONE_MINUTE, 2), contains(now, now.minus(1, MINUTES)));
    }

    public void testCommitUpToIfUnavailable() {
        Mockito.when(featureService.clusterHasFeature(Mockito.any(), Mockito.eq(MeteringPlugin.SAMPLED_METRICS_METADATA)))
            .thenReturn(false);

        assertThat(timeCursor.commitUpTo(Instant.now()), is(true));
    }

    @SuppressWarnings("unchecked")
    public void testCommitUpTo() {
        Mockito.when(featureService.clusterHasFeature(Mockito.any(), Mockito.eq(MeteringPlugin.SAMPLED_METRICS_METADATA))).thenReturn(true);
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
        Mockito.when(featureService.clusterHasFeature(Mockito.any(), Mockito.eq(MeteringPlugin.SAMPLED_METRICS_METADATA))).thenReturn(true);
        Instant timestamp = Instant.now();

        Mockito.doAnswer(invocation -> {
            // complete the listener
            ((ActionListener<ActionResponse.Empty>) invocation.getArguments()[2]).onFailure(new ElasticsearchException("timeout"));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());

        assertThat(timeCursor.commitUpTo(timestamp), is(false));
    }

}
