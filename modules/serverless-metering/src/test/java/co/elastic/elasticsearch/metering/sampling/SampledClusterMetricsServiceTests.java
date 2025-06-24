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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.MockedClusterStateTestUtils;
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.activitytracking.ActivityTests;
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingState;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingStatus;
import co.elastic.elasticsearch.metering.sampling.action.CollectClusterSamplesAction;
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.metering.MockedClusterStateMetadataTestUtils.mockedClusterStateMetadata;
import static co.elastic.elasticsearch.metering.MockedClusterStateMetadataTestUtils.mockedIndex;
import static co.elastic.elasticsearch.metering.ShardInfoMetricsTestUtils.shardInfoMetricsBuilder;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.NODE_INFO_COLLECTIONS_ERRORS_TOTAL;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.NODE_INFO_COLLECTIONS_PARTIALS_TOTAL;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.NODE_INFO_TIER_SEARCH_ACTIVITY_TIME;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.ANOTHER_NODE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
import static java.util.Map.entry;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SampledClusterMetricsServiceTests extends ESTestCase {

    public static final Index INDEX_1 = new Index("index1", "index1UUID");
    public static final ShardId SHARD_0 = new ShardId(INDEX_1, 0);
    public static final ShardId SHARD_1 = new ShardId(INDEX_1, 1);
    public static final ShardId SHARD_2 = new ShardId(INDEX_1, 2);

    public static final Index SYS_INDEX = new Index("sysIndex", "sysIndexUUID");
    public static final ShardId SYS_SHARD_0 = new ShardId(SYS_INDEX, 0);

    public void testClusterChangedUnchanged() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), mock(SystemIndices.class), MeterRegistry.NOOP);
        var initialNodeStatus = randomFrom(PersistentTaskNodeStatus.values());
        var metrics = SampledClusterMetrics.EMPTY.withAdditionalStatus(SamplingStatus.STALE);
        service.metricsState.set(new SamplingState(initialNodeStatus, metrics, Instant.EPOCH));

        var clusterState = MockedClusterStateTestUtils.createMockClusterState(); // unchanged
        service.clusterChanged(new ClusterChangedEvent("TEST", clusterState, clusterState));
        // cluster state is unchanged, so no update to node status
        assertThat(service.metricsState.get(), is(new SamplingState(initialNodeStatus, metrics, Instant.EPOCH)));
    }

    public void testClusterChangedUnchangedAssignment() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), mock(SystemIndices.class), MeterRegistry.NOOP);
        var metrics = SampledClusterMetrics.EMPTY.withAdditionalStatus(SamplingStatus.STALE);
        service.metricsState.set(new SamplingState(THIS_NODE, metrics, Instant.EPOCH));

        var newState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        var oldState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        service.clusterChanged(new ClusterChangedEvent("TEST", newState, oldState));
        // cluster state is unchanged, so no update to node status
        assertThat(service.metricsState.get(), is(new SamplingState(THIS_NODE, metrics, Instant.EPOCH)));
    }

    public void testClusterChangedToThisTaskNode() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), mock(SystemIndices.class), MeterRegistry.NOOP);
        var initialNodeStatus = randomFrom(PersistentTaskNodeStatus.values());
        var metrics = SampledClusterMetrics.EMPTY.withAdditionalStatus(SamplingStatus.STALE);
        service.metricsState.set(new SamplingState(initialNodeStatus, metrics, Instant.EPOCH));

        var clusterState = MockedClusterStateTestUtils.createMockClusterState();
        var previousState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            clusterState,
            randomFrom(MockedClusterStateTestUtils.NON_LOCAL_NODE_ID, null)
        );
        var currentState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            clusterState,
            MockedClusterStateTestUtils.LOCAL_NODE_ID
        );

        service.clusterChanged(new ClusterChangedEvent("TEST", currentState, previousState));
        // cluster state is unchanged, so no update to node status
        assertThat(service.metricsState.get(), is(new SamplingState(THIS_NODE, SampledClusterMetrics.EMPTY, Instant.EPOCH)));
    }

    public void testClusterChangedToOtherTaskNode() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), mock(SystemIndices.class), MeterRegistry.NOOP);
        var initialNodeStatus = randomFrom(PersistentTaskNodeStatus.values());
        var metrics = SampledClusterMetrics.EMPTY.withAdditionalStatus(SamplingStatus.STALE);
        service.metricsState.set(new SamplingState(initialNodeStatus, metrics, Instant.EPOCH));

        var clusterState = MockedClusterStateTestUtils.createMockClusterState();
        var previousState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            clusterState,
            randomFrom(MockedClusterStateTestUtils.LOCAL_NODE_ID, null)
        );
        var currentState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            clusterState,
            MockedClusterStateTestUtils.NON_LOCAL_NODE_ID
        );

        service.clusterChanged(new ClusterChangedEvent("TEST", currentState, previousState));
        // cluster state is unchanged, so no update to node status
        assertThat(service.metricsState.get(), is(new SamplingState(ANOTHER_NODE, SampledClusterMetrics.EMPTY, Instant.EPOCH)));
    }

    public void testSamplesEmpty() {
        var clusterService = createMockClusterService(Set::of);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        assertSamplesNotReady(service, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, Map.of());
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(service, is(anEmptyMap()), is(Map.of()), is(SampledTierMetrics.EMPTY), is(SampledTierMetrics.EMPTY));

        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_TOTAL, is(1L));
    }

    public void testSamplesUpdate() {
        var shardsInfo = Map.ofEntries(
            entry(SHARD_0, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 21L).withGeneration(1, 1, 0).build()),
            entry(SYS_SHARD_0, shardInfoMetricsBuilder().withData(130L, 13L, 0L, 23L).withGeneration(1, 1, 0).build())
        );
        var searchMetrics = randomSampledTierMetrics();
        var indexMetrics = randomSampledTierMetrics();

        var clusterService = createMockClusterService(
            shardsInfo::keySet,
            () -> new SampledMetricsMetadata(Instant.now()),
            index -> index == SYS_INDEX ? mockedIndex(index, true, true, "sysDS") : mockedIndex(index, false, false)
        );
        var systemIndices = mock(SystemIndices.class);
        when(systemIndices.isSystemIndex(SYS_INDEX.getName())).thenReturn(true);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, systemIndices, meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), searchMetrics, indexMetrics, shardsInfo);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(SHARD_0), shardSize(11L)), hasEntry(is(SYS_SHARD_0), shardSize(13L))),
            mapOf(
                hasEntry(is(INDEX_1), both(indexSize(11)).and(indexMetadata(INDEX_1))),
                hasEntry(is(SYS_INDEX), both(indexSize(13)).and(indexMetadata(SYS_INDEX, "sysDS", true, true)))
            ),
            is(searchMetrics),
            is(indexMetrics)
        );

        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_TOTAL, equalTo(1L));
    }

    public void testSamplesUpdatePartialSuccess() {
        var shardsInfo = Map.ofEntries(
            entry(SHARD_0, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 21L).withGeneration(1, 1, 0).build())
        );
        var searchMetrics = new SampledTierMetrics(4 * 1_000_000_000L, ActivityTests.randomActivity());
        var indexMetrics = new SampledTierMetrics(9 * 1_000_000_000L, ActivityTests.randomActivity());

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(
            mock(),
            new CollectClusterSamplesAction.Response(
                searchMetrics.memorySize(),
                indexMetrics.memorySize(),
                searchMetrics.activity(),
                indexMetrics.activity(),
                shardsInfo,
                2,
                1,
                5,
                2
            )
        );
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(SHARD_0), shardSize(11L))),
            mapOf(hasEntry(is(INDEX_1), both(indexSize(11)).and(indexMetadata(INDEX_1)))),
            isExtrapolatedTierMetrics(searchMetrics, 2 * 4 * 1_000_000_000L), // 2 * 4GB, 1 of 2 nodes reported 4 GB total
            isExtrapolatedTierMetrics(indexMetrics, 5 * (9 / 3) * 1_000_000_000L) // 5 * 3GB, 3 of 5 nodes reported 9 GB total
        );
        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_TOTAL, equalTo(1L));
        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_PARTIALS_TOTAL, equalTo(1L));
    }

    private Matcher<SampledTierMetrics> isExtrapolatedTierMetrics(SampledTierMetrics metrics, long extrapolatedMemorySize) {
        return both(transformedMatch(SampledTierMetrics::activity, is(metrics.activity()))).and(
            transformedMatch(SampledTierMetrics::memorySize, is(extrapolatedMemorySize))
        );
    }

    public void testSamplesUpdateError() {
        var clusterService = createMockClusterService(Set::of);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesFailure(mock(), new Exception("Total failure"));
        safeAwaitFailure((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(service, is(anEmptyMap()), is(anEmptyMap()), is(SampledTierMetrics.EMPTY), is(SampledTierMetrics.EMPTY));

        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_TOTAL, equalTo(1L));
        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_ERRORS_TOTAL, equalTo(1L));
    }

    public void testSamplesUpdateWithMerge() {
        var shardsInfo = Map.ofEntries(
            entry(SHARD_0, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(SHARD_1, shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 2, 0).build()),
            entry(SHARD_2, shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(SHARD_1, shardInfoMetricsBuilder().withData(120L, 22L, 0L, 0).withGeneration(1, 3, 0L).build()),
            entry(SHARD_2, shardInfoMetricsBuilder().withData(130L, 23L, 0L, 0).withGeneration(1, 2, 0).build())
        );
        var searchMetrics1 = randomSampledTierMetrics();
        var indexMetrics1 = randomSampledTierMetrics();
        var searchMetrics2 = randomSampledTierMetrics();
        var indexMetrics2 = randomSampledTierMetrics();

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var coolDown = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());

        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), searchMetrics1, indexMetrics1, shardsInfo);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(SHARD_0), shardSize(11L)), hasEntry(is(SHARD_1), shardSize(12L)), hasEntry(is(SHARD_2), shardSize(13L))),
            mapOf(hasEntry(is(INDEX_1), both(indexSize(36)).and(indexMetadata(INDEX_1)))),
            is(searchMetrics1),
            is(indexMetrics1)
        );

        mockCollectClusterSamplesSuccess(client, searchMetrics2, indexMetrics2, shardsInfo2);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(SHARD_0), shardSize(11L)), hasEntry(is(SHARD_1), shardSize(22L)), hasEntry(is(SHARD_2), shardSize(23L))),
            mapOf(hasEntry(is(INDEX_1), both(indexSize(56)).and(indexMetadata(INDEX_1)))),
            is(searchMetrics1.merge(searchMetrics2.memorySize(), searchMetrics2.activity(), coolDown)),
            is(indexMetrics1.merge(indexMetrics2.memorySize(), indexMetrics2.activity(), coolDown))
        );

        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_TOTAL, equalTo(2L));
    }

    public void testSamplesUpdateWhenIndexReplaced() {
        var idx1 = new ShardId("index1", "index1UUID", 0);
        var idx2 = new ShardId("index2", "index2UUID", 0);
        var newIdx1 = new ShardId("index1", "index1UUID-2", 0);

        var shardsInfo = Map.ofEntries(
            entry(idx1, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(idx2, shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 2, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(newIdx1, shardInfoMetricsBuilder().withData(130L, 23L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var activeShards = new AtomicReference<>(shardsInfo.keySet());
        var samplingMetadata = new AtomicReference<>(new SampledMetricsMetadata(Instant.now()));

        var clusterService = createMockClusterService(activeShards::get, samplingMetadata::get); // index of newIdx1 unknown
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(idx1), shardSize(11L)), hasEntry(is(idx2), shardSize(13L))),
            mapOf(
                hasEntry(is(idx1.getIndex()), both(indexSize(11)).and(indexMetadata(idx1.getIndex()))),
                hasEntry(is(idx2.getIndex()), both(indexSize(13)).and(indexMetadata(idx2.getIndex())))
            )
        );

        // update routing table to reflect the change from idx1 to newIdx1
        activeShards.set(Set.of(idx2, newIdx1));
        mockCollectClusterSamplesSuccess(client, SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo2);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(idx1), shardSize(11L)), hasEntry(is(idx2), shardSize(13L)), hasEntry(is(newIdx1), shardSize(23L))),
            mapOf(
                hasEntry(is(idx1.getIndex()), both(indexSize(11)).and(indexMetadata(idx1.getIndex()))),
                hasEntry(is(idx2.getIndex()), both(indexSize(13)).and(indexMetadata(idx2.getIndex()))),
                hasEntry(is(newIdx1.getIndex()), both(indexSize(23)).and(indexMetadata(newIdx1.getIndex())))
            )
        );

        // update sampling metadata so old obsolete shards are removed on the next update
        samplingMetadata.set(new SampledMetricsMetadata(Instant.now()));
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(idx2), shardSize(13L)), hasEntry(is(newIdx1), shardSize(23L))),
            mapOf(
                hasEntry(is(idx2.getIndex()), both(indexSize(13)).and(indexMetadata(idx2.getIndex()))),
                hasEntry(is(newIdx1.getIndex()), both(indexSize(23)).and(indexMetadata(newIdx1.getIndex())))
            )
        );
        assertMeasurementTotal(meterRegistry, InstrumentType.LONG_COUNTER, NODE_INFO_COLLECTIONS_TOTAL, equalTo(3L));
    }

    public void testSamplesUpdateWithMergeSkipsOldGeneration() {
        var shardsInfo = Map.ofEntries(
            entry(SHARD_0, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(SHARD_1, shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 2, 0).build()),
            entry(SHARD_2, shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(SHARD_1, shardInfoMetricsBuilder().withData(120L, 22L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(SHARD_2, shardInfoMetricsBuilder().withData(130L, 23L, 0L, 0).withGeneration(1, 2, 0).build())
        );

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(SHARD_0), shardSize(11L)), hasEntry(is(SHARD_1), shardSize(12L)), hasEntry(is(SHARD_2), shardSize(13L))),
            mapOf(hasEntry(is(INDEX_1), both(indexSize(36)).and(indexMetadata(INDEX_1))))
        );

        mockCollectClusterSamplesSuccess(client, SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo2);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(SHARD_0), shardSize(11L)), hasEntry(is(SHARD_1), shardSize(12L)), hasEntry(is(SHARD_2), shardSize(23L))),
            mapOf(hasEntry(is(INDEX_1), both(indexSize(46)).and(indexMetadata(INDEX_1))))
        );
    }

    public void testSamplesUpdateWhenIndexRemoved() {
        var idx1 = new ShardId("index1", "index1UUID", 0);
        var idx2 = new ShardId("index2", "index2UUID", 0);

        var shardsInfo = Map.ofEntries(
            entry(idx1, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(idx2, shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(entry(idx2, shardInfoMetricsBuilder().withData(120L, 22L, 0L, 0).withGeneration(1, 2, 0).build()));

        var activeShards = new AtomicReference<>(shardsInfo.keySet());
        var samplingMetadata = new AtomicReference<>(new SampledMetricsMetadata(Instant.now()));

        var clusterService = createMockClusterService(activeShards::get, samplingMetadata::get);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(idx1), shardSize(11L)), hasEntry(is(idx2), shardSize(12L))),
            mapOf(
                hasEntry(is(idx1.getIndex()), both(indexSize(11)).and(indexMetadata(idx1.getIndex()))),
                hasEntry(is(idx2.getIndex()), both(indexSize(12)).and(indexMetadata(idx2.getIndex())))
            )
        );

        mockCollectClusterSamplesFailure(client, new Exception("intermittent failures doesn't impact future updates"));
        safeAwaitFailure((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        // Update routing table to remove INDEX_1. Though, it will be kept until the committed timestamp proceeded.
        activeShards.set(shardsInfo2.keySet());
        mockCollectClusterSamplesSuccess(client, SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo2);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(idx1), shardSize(11L)), hasEntry(is(idx2), shardSize(22L))),
            mapOf(
                hasEntry(is(idx1.getIndex()), both(indexSize(11)).and(indexMetadata(idx1.getIndex()))),
                hasEntry(is(idx2.getIndex()), both(indexSize(22)).and(indexMetadata(idx2.getIndex())))
            )
        );

        // Index 1 is finally removed after the committed timestamp proceeded.
        samplingMetadata.set(new SampledMetricsMetadata(Instant.now()));

        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(
            service,
            mapOf(hasEntry(is(idx2), shardSize(22L))),
            mapOf(hasEntry(is(idx2.getIndex()), both(indexSize(22)).and(indexMetadata(idx2.getIndex()))))
        );
    }

    public void testPersistentTaskNodeChangeResetShardInfo() {
        var shardsInfo = Map.ofEntries(
            entry(SHARD_0, shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(SHARD_1, shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 2, 0).build()),
            entry(SHARD_2, shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var client = mockCollectClusterSamplesSuccess(mock(), SampledTierMetrics.EMPTY, SampledTierMetrics.EMPTY, shardsInfo);
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        assertSamplesReady(service, is(aMapWithSize(3)), is(aMapWithSize(1)));

        var previousState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        var currentState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            previousState,
            MockedClusterStateTestUtils.NON_LOCAL_NODE_ID
        );
        service.clusterChanged(new ClusterChangedEvent("TEST", currentState, previousState));

        assertSamplesNotReady(service, ANOTHER_NODE);
    }

    public void testActivityMetrics() {
        var clusterService = createMockClusterService(Set::of);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        var searchActivity = randomBoolean()
            ? ActivityTests.randomActivityActive(Duration.ofMinutes(1))
            : ActivityTests.randomActivityNotActive();
        var indexActivity = randomBoolean()
            ? ActivityTests.randomActivityActive(Duration.ofMinutes(1))
            : ActivityTests.randomActivityNotActive();

        // Update sample to non-empty Activity
        var client = mockCollectClusterSamplesSuccess(
            mock(),
            new CollectClusterSamplesAction.Response(0, 0, searchActivity, indexActivity, Map.of(), 1, 0, 1, 0)
        );
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        meterRegistry.getRecorder().collect();
        var searchMetrics = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
        var indexMetrics = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_INDEX_ACTIVITY_TIME);
        assertThat(searchMetrics.size(), equalTo(1));
        assertThat(indexMetrics.size(), equalTo(1));
        assertActivityMetrics(searchActivity, searchMetrics.getFirst());
        assertActivityMetrics(indexActivity, indexMetrics.getFirst());
    }

    public void testActivityMetricsEmpty() {
        var clusterService = createMockClusterService(Set::of);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, mock(SystemIndices.class), meterRegistry, THIS_NODE);

        // No metrics returned initially when activity is empty
        meterRegistry.getRecorder().collect();
        var searchMetrics1 = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
        var indexMetrics1 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_INDEX_ACTIVITY_TIME);
        assertThat(searchMetrics1, empty());
        assertThat(indexMetrics1, empty());

        // Update sample to non-empty Activity
        var client = mockCollectClusterSamplesSuccess(
            mock(),
            new CollectClusterSamplesAction.Response(
                0,
                0,
                ActivityTests.randomActivityNotEmpty(),
                ActivityTests.randomActivityNotEmpty(),
                Map.of(),
                1,
                0,
                1,
                0
            )
        );
        safeAwait((ActionListener<Void> listener) -> service.updateSamples(client, listener));

        // Checks metrics now show activity
        meterRegistry.getRecorder().collect();
        var searchMetrics2 = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
        var indexMetrics2 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_INDEX_ACTIVITY_TIME);
        assertThat(searchMetrics2, not(empty()));
        assertThat(indexMetrics2, not(empty()));

        // Change persistent task node
        var previousState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        var currentState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            previousState,
            MockedClusterStateTestUtils.NON_LOCAL_NODE_ID
        );
        service.clusterChanged(new ClusterChangedEvent("TEST", currentState, previousState));

        // Check no new activity
        meterRegistry.getRecorder().collect();
        var searchMetrics3 = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
        var indexMetrics3 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_INDEX_ACTIVITY_TIME);
        assertThat(searchMetrics3, equalTo(searchMetrics2));
        assertThat(indexMetrics3, equalTo(indexMetrics2));
    }

    private ClusterService createMockClusterService(Supplier<Set<ShardId>> shardsInfo) {
        return createMockClusterService(shardsInfo, () -> null);
    }

    private ClusterService createMockClusterService(Supplier<Set<ShardId>> shardsInfo, Supplier<SampledMetricsMetadata> samplingMetadata) {
        return createMockClusterService(shardsInfo, samplingMetadata, index -> mockedIndex(index, false, false));
    }

    private ClusterService createMockClusterService(
        Supplier<Set<ShardId>> shardsInfo,
        Supplier<SampledMetricsMetadata> samplingMetadata,
        Function<Index, IndexAbstraction> indexToIndexAbstraction
    ) {
        var clusterService = mock(ClusterService.class);
        var clusterState = mock(ClusterState.class);
        var metadata = mock(Metadata.class);
        var routingTable = mock(RoutingTable.class);

        when(routingTable.iterator()).thenAnswer(a -> {
            Map<Index, IndexRoutingTable.Builder> table = new HashMap<>();
            for (var shardId : shardsInfo.get()) {
                table.computeIfAbsent(shardId.getIndex(), idx -> IndexRoutingTable.builder(idx))
                    .addShard(TestShardRouting.newShardRouting(shardId, "node_0", true, ShardRoutingState.STARTED));
            }
            return table.values().stream().map(IndexRoutingTable.Builder::build).iterator();
        });
        mockedClusterStateMetadata(metadata, () -> {
            Set<ShardId> shardIds = shardsInfo.get();
            Map<Index, IndexAbstraction> indexAbstractions = new HashMap<>();
            for (var shardId : shardIds) {
                indexAbstractions.computeIfAbsent(shardId.getIndex(), idx -> indexToIndexAbstraction.apply(idx));
            }
            return indexAbstractions.values();
        });

        when(routingTable.allShards()).thenAnswer(
            a -> shardsInfo.get().stream().map(s -> TestShardRouting.newShardRouting(s, "node_0", true, ShardRoutingState.STARTED))
        );
        final var globalRoutingTable = GlobalRoutingTable.builder().put(Metadata.DEFAULT_PROJECT_ID, routingTable).build();
        when(clusterState.globalRoutingTable()).thenReturn(globalRoutingTable);
        when(clusterState.custom(SampledMetricsMetadata.TYPE)).thenAnswer(i -> samplingMetadata.get());
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return clusterService;
    }

    private void assertSamplesNotReady(SampledClusterMetricsService service, PersistentTaskNodeStatus nodeStatus) {
        service.withSamplesIfReady(metrics -> {
            fail("should not be ready");
            return metrics;
        }, status -> {});
        assertThat(service.getMeteringShardInfo(), equalTo(SampledClusterMetrics.EMPTY));
    }

    private void assertSamplesReady(
        SampledClusterMetricsService service,
        Matcher<Map<? extends ShardId, ? extends ShardInfoMetrics>> shardInfosMatcher,
        Matcher<Map<? extends Index, ? extends IndexInfoMetrics>> indexInfosMatcher
    ) {
        assertSamplesReady(
            service,
            shardInfosMatcher,
            indexInfosMatcher,
            Matchers.any(SampledTierMetrics.class),
            Matchers.any(SampledTierMetrics.class)
        );
    }

    private void assertSamplesReady(
        SampledClusterMetricsService service,
        Matcher<Map<? extends ShardId, ? extends ShardInfoMetrics>> shardInfosMatcher,
        Matcher<Map<? extends Index, ? extends IndexInfoMetrics>> indexInfosMatcher,
        Matcher<SampledTierMetrics> searchTierMetricsMatcher,
        Matcher<SampledTierMetrics> indexTierMetricsMatcher
    ) {
        service.withSamplesIfReady(samples -> {
            assertThat(samples, equalTo(service.getMeteringShardInfo()));
            assertThat(samples.storageMetrics().getShardInfos(), shardInfosMatcher);
            assertThat(samples.storageMetrics().getIndexInfos(), indexInfosMatcher);
            assertThat(samples.searchTierMetrics(), searchTierMetricsMatcher);
            assertThat(samples.indexTierMetrics(), indexTierMetricsMatcher);
            return null;
        }, status -> fail("should be ready"));
    }

    private void assertMeasurementTotal(RecordingMeterRegistry registry, InstrumentType type, String name, Matcher<Long> matcher) {
        registry.getRecorder().collect();
        var values = Measurement.combine(registry.getRecorder().getMeasurements(type, name)).stream().map(m -> m.getLong()).toList();
        assertThat(values, contains(matcher));
    }

    private Matcher<ShardInfoMetrics> shardSize(final long totalSize) {
        return new FeatureMatcher<>(equalTo(totalSize), "shard size", "totalSize") {
            @Override
            protected Long featureValueOf(ShardInfoMetrics actual) {
                return actual.totalSizeInBytes();
            }
        };
    }

    private Matcher<IndexInfoMetrics> indexSize(final long totalSize) {
        return new FeatureMatcher<>(equalTo(totalSize), "index size", "totalSize") {
            @Override
            protected Long featureValueOf(IndexInfoMetrics actual) {
                return actual.getTotalSize();
            }
        };
    }

    private Matcher<IndexInfoMetrics> indexMetadata(Index index) {
        return indexMetadata(index, null, false, false);
    }

    private Matcher<IndexInfoMetrics> indexMetadata(Index index, String datastream, boolean isSystem, boolean isHidden) {
        var metadata = Map.ofEntries(
            entry("index", index.getName()),
            entry("index_uuid", index.getUUID()),
            entry("system_index", Boolean.toString(isSystem)),
            entry("hidden_index", Boolean.toString(isHidden))
        );
        return indexMetadata(datastream != null ? Maps.copyMapWithAddedEntry(metadata, "datastream", datastream) : metadata);
    }

    private Matcher<IndexInfoMetrics> indexMetadata(final Map<String, String> sourceMetadata) {
        return new FeatureMatcher<>(is(sourceMetadata), "index metadata", "sourceMetadata") {
            @Override
            protected Map<String, String> featureValueOf(IndexInfoMetrics actual) {
                return actual.getSourceMetadata();
            }
        };
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private <K, V> Matcher<Map<? extends K, ? extends V>> mapOf(Matcher<Map<? extends K, ? extends V>>... matchers) {
        if (matchers.length == 0) {
            return anEmptyMap();
        } else {
            return allOf(ArrayUtils.append(matchers, aMapWithSize(matchers.length)));
        }
    }

    private Client mockCollectClusterSamples(Client mock, Consumer<ActionListener<CollectClusterSamplesAction.Response>> onCompletion) {
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            onCompletion.accept(listener);
            return null;
        }).when(mock).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());
        return mock;
    }

    private Client mockCollectClusterSamplesSuccess(
        Client mock,
        SampledTierMetrics searchTier,
        SampledTierMetrics indexTier,
        Map<ShardId, ShardInfoMetrics> shardsInfo
    ) {
        return mockCollectClusterSamples(
            mock,
            l -> l.onResponse(
                new CollectClusterSamplesAction.Response(
                    searchTier.memorySize(),
                    indexTier.memorySize(),
                    searchTier.activity(),
                    indexTier.activity(),
                    shardsInfo,
                    1,
                    0,
                    1,
                    0
                )
            )
        );
    }

    private Client mockCollectClusterSamplesSuccess(Client mock, CollectClusterSamplesAction.Response response) {
        return mockCollectClusterSamples(mock, l -> l.onResponse(response));
    }

    private Client mockCollectClusterSamplesFailure(Client mock, Exception exception) {
        return mockCollectClusterSamples(mock, l -> l.onFailure(exception));
    }

    public static SampledTierMetrics randomSampledTierMetrics() {
        return new SampledTierMetrics(randomNonNegativeLong(), ActivityTests.randomActivity());
    }

    private void assertActivityMetrics(Activity activity, Measurement measurement) {
        var now = Instant.now();
        var sinceFirstInPeriod = Duration.between(activity.firstActivityRecentPeriod(), now).getSeconds();
        var sinceLastInPeriod = Duration.between(activity.lastActivityRecentPeriod(), now).minus(ActivityTests.COOL_DOWN).getSeconds();
        if (activity.isBeforeLastCoolDownExpires(now, ActivityTests.COOL_DOWN)) {
            assertThat((double) measurement.getLong(), is(closeTo(sinceFirstInPeriod, 1)));
        } else {
            assertThat((double) measurement.getLong(), is(closeTo(-sinceLastInPeriod, 1)));
        }
    }
}
