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
import co.elastic.elasticsearch.metering.ShardInfoMetricsTestUtils;
import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.activitytracking.ActivityTests;
import co.elastic.elasticsearch.metering.activitytracking.TaskActivityTracker;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledClusterMetrics;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledShardInfos;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SampledTierMetrics;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingState;
import co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.SamplingStatus;
import co.elastic.elasticsearch.metering.sampling.action.CollectClusterSamplesAction;
import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.ANOTHER_NODE;
import static co.elastic.elasticsearch.metering.sampling.SampledClusterMetricsService.PersistentTaskNodeStatus.THIS_NODE;
import static java.util.Map.entry;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SampledClusterMetricsServiceTests extends ESTestCase {
    public void testEmptyShardInfo() {
        var clusterService = createMockClusterService(Set::of);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();
        var initialSearchMetrics = service.getSearchTierMetrics();
        var initialIndexMetrics = service.getIndexTierMetrics();

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(new CollectClusterSamplesAction.Response(0, 0, Activity.EMPTY, Activity.EMPTY, Map.of(), List.of()));
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();
        var firstSearchMetrics = service.getSearchTierMetrics();
        var firstIndexMetrics = service.getIndexTierMetrics();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(initialSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(initialIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(firstRoundShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(firstSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(firstIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        final List<Measurement> measurements = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL)
        );
        assertThat(measurements, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
    }

    public void testClusterChangedUnchanged() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), MeterRegistry.NOOP);
        var initialNodeStatus = randomFrom(PersistentTaskNodeStatus.values());
        var metrics = SampledClusterMetrics.EMPTY.withAdditionalStatus(SamplingStatus.STALE);
        service.metricsState.set(new SamplingState(initialNodeStatus, metrics, Instant.EPOCH));

        var clusterState = MockedClusterStateTestUtils.createMockClusterState(); // unchanged
        service.clusterChanged(new ClusterChangedEvent("TEST", clusterState, clusterState));
        // cluster state is unchanged, so no update to node status
        assertThat(service.metricsState.get(), is(new SamplingState(initialNodeStatus, metrics, Instant.EPOCH)));
    }

    public void testClusterChangedUnchangedAssignment() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), MeterRegistry.NOOP);
        var metrics = SampledClusterMetrics.EMPTY.withAdditionalStatus(SamplingStatus.STALE);
        service.metricsState.set(new SamplingState(THIS_NODE, metrics, Instant.EPOCH));

        var newState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        var oldState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        service.clusterChanged(new ClusterChangedEvent("TEST", newState, oldState));
        // cluster state is unchanged, so no update to node status
        assertThat(service.metricsState.get(), is(new SamplingState(THIS_NODE, metrics, Instant.EPOCH)));
    }

    public void testClusterChangedToThisTaskNode() {
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), MeterRegistry.NOOP);
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
        var service = new SampledClusterMetricsService(createMockClusterService(Set::of), MeterRegistry.NOOP);
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

    public void testInitialShardInfoUpdate() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 21L).withGeneration(1, 1, 0).build()
            ),
            entry(
                shard2Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 22L).withGeneration(1, 2, 0L).build()
            ),
            entry(
                shard3Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 23L).withGeneration(1, 1, 0).build()
            )
        );
        var searchMetrics = randomSampledTierMetrics();
        var indexMetrics = randomSampledTierMetrics();

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();
        var initialSearchMetrics = service.getSearchTierMetrics();
        var initialIndexMetrics = service.getIndexTierMetrics();

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(
                new CollectClusterSamplesAction.Response(
                    searchMetrics.memorySize(),
                    indexMetrics.memorySize(),
                    searchMetrics.activity(),
                    indexMetrics.activity(),
                    shardsInfo,
                    List.of()
                )
            );
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();
        var firstSearchMetrics = service.getSearchTierMetrics();
        var firstIndexMetrics = service.getIndexTierMetrics();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(initialSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(initialIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo,
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard2Id, withSizeInBytes(12L)),
                entry(shard3Id, withSizeInBytes(13L))
            )
        );
        assertThat(firstSearchMetrics, equalTo(searchMetrics));
        assertThat(firstIndexMetrics, equalTo(indexMetrics));

        final List<Measurement> measurements = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL)
        );
        assertThat(measurements, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
    }

    public void testPartialShardInfoUpdate() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 21L).withGeneration(1, 1, 0).build()
            ),
            entry(
                shard2Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 22L).withGeneration(1, 2, 0).build()
            ),
            entry(
                shard3Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 23L).withGeneration(1, 1, 0).build()
            )
        );
        var searchMetrics = randomSampledTierMetrics();
        var indexMetrics = randomSampledTierMetrics();

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();
        var initialSearchMetrics = service.getSearchTierMetrics();
        var initialIndexMetrics = service.getIndexTierMetrics();

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(
                new CollectClusterSamplesAction.Response(
                    searchMetrics.memorySize(),
                    indexMetrics.memorySize(),
                    searchMetrics.activity(),
                    indexMetrics.activity(),
                    shardsInfo,
                    List.of(new Exception("Partial failure"))
                )
            );
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();
        var firstSearchMetrics = service.getSearchTierMetrics();
        var firstIndexMetrics = service.getIndexTierMetrics();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(initialSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(initialIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo,
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard2Id, withSizeInBytes(12L)),
                entry(shard3Id, withSizeInBytes(13L))
            )
        );
        assertThat(firstSearchMetrics, equalTo(searchMetrics));
        assertThat(firstIndexMetrics, equalTo(indexMetrics));

        final List<Measurement> collections = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL)
        );
        final List<Measurement> partials = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_PARTIALS_TOTAL)
        );
        assertThat(collections, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(partials, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
    }

    public void testErrorShardInfoUpdate() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(
                shard1Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 21L).withGeneration(1, 1, 0).build()
            ),
            entry(
                shard2Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 22L).withGeneration(1, 2, 0).build()
            ),
            entry(
                shard3Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 23L).withGeneration(1, 1, 0).build()
            )
        );

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();
        var initialSearchMetrics = service.getSearchTierMetrics();
        var initialIndexMetrics = service.getIndexTierMetrics();

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onFailure(new Exception("Total failure"));
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();
        var firstSearchMetrics = service.getSearchTierMetrics();
        var firstIndexMetrics = service.getIndexTierMetrics();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(initialSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(initialIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(firstRoundShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(firstSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(firstIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        final List<Measurement> collections = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL)
        );
        final List<Measurement> errors = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_ERRORS_TOTAL)
        );
        assertThat(collections, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
        assertThat(errors, contains(transformedMatch(Measurement::getLong, equalTo(1L))));
    }

    public void testShardInfoDiffUpdate() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(shard2Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 2, 0).build()),
            entry(shard3Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(
                shard2Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 22L, 0L, 0).withGeneration(1, 3, 0L).build()
            ),
            entry(shard3Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 23L, 0L, 0).withGeneration(1, 2, 0).build())
        );
        var searchMetrics1 = randomSampledTierMetrics();
        var indexMetrics1 = randomSampledTierMetrics();
        var searchMetrics2 = randomSampledTierMetrics();
        var indexMetrics2 = randomSampledTierMetrics();

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();
        var initialSearchMetrics = service.getSearchTierMetrics();
        var initialIndexMetrics = service.getIndexTierMetrics();

        var client = mock(Client.class);
        doAnswer(
            new TestCollectClusterSamplesActionAnswer(searchMetrics1, indexMetrics1, searchMetrics2, indexMetrics2, shardsInfo, shardsInfo2)
        ).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);
        var firstRoundShardInfo = service.getMeteringShardInfo();
        var firstSearchMetrics = service.getSearchTierMetrics();
        var firstIndexMetrics = service.getIndexTierMetrics();

        service.updateSamples(client);
        var secondRoundShardInfo = service.getMeteringShardInfo();
        var secondSearchMetrics = service.getSearchTierMetrics();
        var secondIndexMetrics = service.getIndexTierMetrics();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));
        assertThat(initialSearchMetrics, equalTo(SampledTierMetrics.EMPTY));
        assertThat(initialIndexMetrics, equalTo(SampledTierMetrics.EMPTY));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo,
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard2Id, withSizeInBytes(12L)),
                entry(shard3Id, withSizeInBytes(13L))
            )
        );
        assertThat(firstSearchMetrics, equalTo(searchMetrics1));
        assertThat(firstIndexMetrics, equalTo(indexMetrics1));

        assertThat(
            secondRoundShardInfo,
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard2Id, withSizeInBytes(22L)),
                entry(shard3Id, withSizeInBytes(23L))
            )
        );

        var coolDown = Duration.ofMillis(TaskActivityTracker.COOL_DOWN_PERIOD.get(clusterService.getSettings()).millis());
        assertThat(
            secondSearchMetrics,
            equalTo(
                new SampledTierMetrics(
                    searchMetrics2.memorySize(),
                    Activity.merge(Stream.of(searchMetrics1.activity(), searchMetrics2.activity()), coolDown)
                )
            )
        );
        assertThat(
            secondIndexMetrics,
            equalTo(
                new SampledTierMetrics(
                    indexMetrics2.memorySize(),
                    Activity.merge(Stream.of(indexMetrics1.activity(), indexMetrics2.activity()), coolDown)
                )
            )
        );

        final List<Measurement> collections = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL)
        );
        assertThat(collections, contains(transformedMatch(Measurement::getLong, equalTo(2L))));
    }

    public void testShardInfoDiffUpdateWithNewUUID() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard3Id = new ShardId("index1", "index1UUID", 3);
        var newShard3Id = new ShardId("index1", "index1UUID-2", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(shard3Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 2, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(
                newShard3Id,
                ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 23L, 0L, 0).withGeneration(1, 1, 0).build()
            )
        );

        var activeShards = new AtomicReference<>(shardsInfo.keySet());
        var samplingMetadata = new AtomicReference<>(new SampledMetricsMetadata(Instant.now()));

        var clusterService = createMockClusterService(activeShards::get, samplingMetadata::get); // index of newShard3Id unknown
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        assertThat(service.getMeteringShardInfo(), containsShardInfos(anEmptyMap()));

        var client = mock(Client.class);
        doAnswer(new TestCollectClusterSamplesActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);
        assertThat(
            service.getMeteringShardInfo(),
            containsShardInfos(entry(shard1Id, withSizeInBytes(11L)), entry(shard3Id, withSizeInBytes(13L)))
        );

        // update routing table to reflect the change from shard3Id to newShard3Id
        activeShards.set(Set.of(shard1Id, newShard3Id));

        service.updateSamples(client);
        assertThat(
            service.getMeteringShardInfo(),
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard3Id, withSizeInBytes(13L)),
                entry(newShard3Id, withSizeInBytes(23L))
            )
        );

        // update sampling metadata so old obsolete shards are removed on the next update
        samplingMetadata.set(new SampledMetricsMetadata(Instant.now()));

        service.updateSamples(client);
        assertThat(
            service.getMeteringShardInfo(),
            containsShardInfos(entry(shard1Id, withSizeInBytes(11L)), entry(newShard3Id, withSizeInBytes(23L)))
        );

        final List<Measurement> collections = Measurement.combine(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, SampledClusterMetricsService.NODE_INFO_COLLECTIONS_TOTAL)
        );
        assertThat(collections, contains(transformedMatch(Measurement::getLong, equalTo(3L))));
    }

    private ClusterService createMockClusterService(Supplier<Set<ShardId>> shardsInfo) {
        return createMockClusterService(shardsInfo, () -> null);
    }

    private ClusterService createMockClusterService(Supplier<Set<ShardId>> shardsInfo, Supplier<SampledMetricsMetadata> samplingMetadata) {
        var clusterService = mock(ClusterService.class);
        var clusterState = mock(ClusterState.class);
        var routingTable = mock(RoutingTable.class);
        when(routingTable.allShards()).thenAnswer(
            a -> shardsInfo.get().stream().map(s -> TestShardRouting.newShardRouting(s, "node_0", true, ShardRoutingState.STARTED))
        );
        final var globalRoutingTable = GlobalRoutingTable.builder().put(Metadata.DEFAULT_PROJECT_ID, routingTable).build();
        when(clusterState.globalRoutingTable()).thenReturn(globalRoutingTable);
        when(clusterState.custom(SampledMetricsMetadata.TYPE)).thenAnswer(i -> samplingMetadata.get());
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return clusterService;
    }

    public void testShardInfoUpdateWithLatest() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(shard2Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 2, 0).build()),
            entry(shard3Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 22L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(shard3Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 23L, 0L, 0).withGeneration(1, 2, 0).build())
        );

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();

        var client = mock(Client.class);
        doAnswer(new TestCollectClusterSamplesActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);
        var firstRoundShardInfo = service.getMeteringShardInfo();

        service.updateSamples(client);
        var secondRoundShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(
            firstRoundShardInfo,
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard2Id, withSizeInBytes(12L)),
                entry(shard3Id, withSizeInBytes(13L))
            )
        );

        assertThat(
            secondRoundShardInfo,
            containsShardInfos(
                entry(shard1Id, withSizeInBytes(11L)),
                entry(shard2Id, withSizeInBytes(12L)),
                entry(shard3Id, withSizeInBytes(23L))
            )
        );
    }

    public void testShardInfoUpdateWhenIndexRemoved() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index2", "index2UUID", 2);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(shard2Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var shardsInfo2 = Map.ofEntries(
            entry(shard2Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 22L, 0L, 0).withGeneration(1, 2, 0).build())
        );

        var activeShards = new AtomicReference<>(shardsInfo.keySet());
        var samplingMetadata = new AtomicReference<>(new SampledMetricsMetadata(Instant.now()));

        var clusterService = createMockClusterService(activeShards::get, samplingMetadata::get);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        assertThat(service.getMeteringShardInfo(), containsShardInfos(anEmptyMap()));

        var client = mock(Client.class);
        doAnswer(new TestCollectClusterSamplesActionAnswer(shardsInfo, shardsInfo2)).when(client)
            .execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);
        assertThat(
            service.getMeteringShardInfo(),
            containsShardInfos(entry(shard1Id, withSizeInBytes(11L)), entry(shard2Id, withSizeInBytes(12L)))
        );

        // Update routing table to remove shard1. Though, it will be kept until the committed timestamp proceeded.
        activeShards.set(shardsInfo2.keySet());

        service.updateSamples(client);
        assertThat(
            service.getMeteringShardInfo(),
            containsShardInfos(entry(shard1Id, withSizeInBytes(11L)), entry(shard2Id, withSizeInBytes(22L)))
        );

        // Shard1 is finally removed after the committed timestamp proceeded.
        samplingMetadata.set(new SampledMetricsMetadata(Instant.now()));

        service.updateSamples(client);
        assertThat(service.getMeteringShardInfo(), containsShardInfos(entry(shard2Id, withSizeInBytes(22L))));
    }

    public void testPersistentTaskNodeChangeResetShardInfo() {
        var shard1Id = new ShardId("index1", "index1UUID", 1);
        var shard2Id = new ShardId("index1", "index1UUID", 2);
        var shard3Id = new ShardId("index1", "index1UUID", 3);

        var shardsInfo = Map.ofEntries(
            entry(shard1Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(110L, 11L, 0L, 0).withGeneration(1, 1, 0).build()),
            entry(shard2Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(120L, 12L, 0L, 0).withGeneration(1, 2, 0).build()),
            entry(shard3Id, ShardInfoMetricsTestUtils.shardInfoMetricsBuilder().withData(130L, 13L, 0L, 0).withGeneration(1, 1, 0).build())
        );

        var clusterService = createMockClusterService(shardsInfo::keySet);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);
        var initialShardInfo = service.getMeteringShardInfo();

        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(new CollectClusterSamplesAction.Response(0, 0, Activity.EMPTY, Activity.EMPTY, shardsInfo, List.of()));
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());

        service.updateSamples(client);

        var firstRoundShardInfo = service.getMeteringShardInfo();

        var previousState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(MockedClusterStateTestUtils.LOCAL_NODE_ID);
        var currentState = MockedClusterStateTestUtils.createMockClusterStateWithPersistentTask(
            previousState,
            MockedClusterStateTestUtils.NON_LOCAL_NODE_ID
        );
        service.clusterChanged(new ClusterChangedEvent("TEST", currentState, previousState));

        var afterNodeChangeShardInfo = service.getMeteringShardInfo();

        assertThat(initialShardInfo, not(nullValue()));
        assertThat(initialShardInfo, containsShardInfos(anEmptyMap()));

        assertThat(firstRoundShardInfo, not(nullValue()));
        assertThat(firstRoundShardInfo, containsShardInfos(aMapWithSize(3)));

        assertThat(afterNodeChangeShardInfo, not(nullValue()));
        assertThat(afterNodeChangeShardInfo, containsShardInfos(anEmptyMap()));
    }

    public void testActivityMetrics() {
        var clusterService = createMockClusterService(Set::of);
        var meterRegistry = new RecordingMeterRegistry();
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);

        var searchActivity = randomBoolean()
            ? ActivityTests.randomActivityActive(Duration.ofMinutes(1))
            : ActivityTests.randomActivityNotActive();
        var indexActivity = randomBoolean()
            ? ActivityTests.randomActivityActive(Duration.ofMinutes(1))
            : ActivityTests.randomActivityNotActive();

        // Update sample to non-empty Activity
        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(new CollectClusterSamplesAction.Response(0, 0, searchActivity, indexActivity, Map.of(), List.of()));
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());
        service.updateSamples(client);

        meterRegistry.getRecorder().collect();
        var searchMetrics = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
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
        var service = new SampledClusterMetricsService(clusterService, meterRegistry, THIS_NODE);

        // No metrics returned initially when activity is empty
        meterRegistry.getRecorder().collect();
        var searchMetrics1 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
        var indexMetrics1 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_INDEX_ACTIVITY_TIME);
        assertThat(searchMetrics1, empty());
        assertThat(indexMetrics1, empty());

        // Update sample to non-empty Activity
        var client = mock(Client.class);
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            listener.onResponse(
                new CollectClusterSamplesAction.Response(
                    0,
                    0,
                    ActivityTests.randomActivityNotEmpty(),
                    ActivityTests.randomActivityNotEmpty(),
                    Map.of(),
                    List.of()
                )
            );
            return null;
        }).when(client).execute(eq(CollectClusterSamplesAction.INSTANCE), any(), any());
        service.updateSamples(client);

        // Checks metrics now show activity
        meterRegistry.getRecorder().collect();
        var searchMetrics2 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
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
        var searchMetrics3 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_SEARCH_ACTIVITY_TIME);
        var indexMetrics3 = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, SampledClusterMetricsService.NODE_INFO_TIER_INDEX_ACTIVITY_TIME);
        assertThat(searchMetrics3, equalTo(searchMetrics2));
        assertThat(indexMetrics3, equalTo(indexMetrics2));
    }

    @SafeVarargs
    private Matcher<SampledShardInfos> containsShardInfos(Map.Entry<ShardId, Matcher<ShardInfoMetrics>>... entryMatchers) {
        List<Matcher<Map<? extends ShardId, ? extends ShardInfoMetrics>>> matchers = new ArrayList<>(entryMatchers.length + 1);
        matchers.add(aMapWithSize(entryMatchers.length));
        for (var entryMatcher : entryMatchers) {
            matchers.add(hasEntry(is(entryMatcher.getKey()), entryMatcher.getValue()));
        }
        return containsShardInfos(matchers);
    }

    private Matcher<SampledShardInfos> containsShardInfos(Matcher<Map<? extends ShardId, ? extends ShardInfoMetrics>> matcher) {
        return containsShardInfos(List.of(matcher));
    }

    @SuppressWarnings("unchecked") // required for allOf(matchers), safe to do here
    private Matcher<SampledShardInfos> containsShardInfos(List<Matcher<Map<? extends ShardId, ? extends ShardInfoMetrics>>> matchers) {
        return new FeatureMatcher<SampledShardInfos, Map<ShardId, ShardInfoMetrics>>(allOf((List) matchers), "shard infos", "shardInfos") {
            @Override
            protected Map<ShardId, ShardInfoMetrics> featureValueOf(SampledShardInfos actual) {
                if (actual instanceof SampledClusterMetrics infos) {
                    return infos.shardSamples();
                } else {
                    throw new AssertionError("Expected SampledClusterMetrics, but got " + actual.getClass());
                }
            }
        };
    }

    private Matcher<ShardInfoMetrics> withSizeInBytes(final long expected) {
        return new FeatureMatcher<>(equalTo(expected), "shard info with size", "size") {
            @Override
            protected Long featureValueOf(ShardInfoMetrics actual) {
                return actual.totalSizeInBytes();
            }
        };
    }

    private static class TestCollectClusterSamplesActionAnswer implements Answer<Object> {
        private final AtomicInteger requestNumber = new AtomicInteger();
        private final SampledTierMetrics searchMetrics1;
        private final SampledTierMetrics indexMetrics1;
        private final SampledTierMetrics searchMetrics2;
        private final SampledTierMetrics indexMetrics2;
        private final Map<ShardId, ShardInfoMetrics> shardsInfo;
        private final Map<ShardId, ShardInfoMetrics> shardsInfo2;

        TestCollectClusterSamplesActionAnswer(Map<ShardId, ShardInfoMetrics> shardsInfo, Map<ShardId, ShardInfoMetrics> shardsInfo2) {
            this(
                SampledTierMetrics.EMPTY,
                SampledTierMetrics.EMPTY,
                SampledTierMetrics.EMPTY,
                SampledTierMetrics.EMPTY,
                shardsInfo,
                shardsInfo2
            );
        }

        TestCollectClusterSamplesActionAnswer(
            SampledTierMetrics searchMetrics1,
            SampledTierMetrics indexMetrics1,
            SampledTierMetrics searchMetrics2,
            SampledTierMetrics indexMetrics2,
            Map<ShardId, ShardInfoMetrics> shardsInfo,
            Map<ShardId, ShardInfoMetrics> shardsInfo2
        ) {
            this.searchMetrics1 = searchMetrics1;
            this.indexMetrics1 = indexMetrics1;
            this.searchMetrics2 = searchMetrics2;
            this.indexMetrics2 = indexMetrics2;
            this.shardsInfo = shardsInfo;
            this.shardsInfo2 = shardsInfo2;
        }

        @Override
        public Object answer(InvocationOnMock answer) {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<CollectClusterSamplesAction.Response>) answer.getArgument(2, ActionListener.class);
            var currentRequest = requestNumber.addAndGet(1);
            if (currentRequest == 1) {
                listener.onResponse(
                    new CollectClusterSamplesAction.Response(
                        searchMetrics1.memorySize(),
                        indexMetrics1.memorySize(),
                        searchMetrics1.activity(),
                        indexMetrics1.activity(),
                        shardsInfo,
                        List.of()
                    )
                );
            } else {
                listener.onResponse(
                    new CollectClusterSamplesAction.Response(
                        searchMetrics2.memorySize(),
                        indexMetrics2.memorySize(),
                        searchMetrics2.activity(),
                        indexMetrics2.activity(),
                        shardsInfo2,
                        List.of()
                    )
                );
            }
            return null;
        }
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
