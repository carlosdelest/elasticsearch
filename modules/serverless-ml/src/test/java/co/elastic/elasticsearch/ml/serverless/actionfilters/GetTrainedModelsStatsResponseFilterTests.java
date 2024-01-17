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

package co.elastic.elasticsearch.ml.serverless.actionfilters;

import co.elastic.elasticsearch.ml.serverless.ServerlessMachineLearningExtension;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction.Response.TrainedModelStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStatsTests;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStatsTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStatsTests;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStatsTests.randomNodeStats;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GetTrainedModelsStatsResponseFilterTests extends ESTestCase {

    public void testRollupDeploymentStats() {
        String id = "model_id";
        TrainedModelStats stats = new TrainedModelStats(
            id,
            TrainedModelSizeStatsTests.createRandom(),
            null,
            randomIntBetween(0, 10),
            InferenceStatsTests.createTestInstance(id, "node_1"),
            AssignmentStatsTests.randomDeploymentStats()
        );
        TrainedModelStats rolledUpStats = GetTrainedModelsStatsResponseFilter.rollupDeploymentStats(stats);
        assertThat(rolledUpStats.getDeploymentStats().getNodeStats(), hasSize(1));
        assertThat(
            rolledUpStats.getDeploymentStats().getNodeStats().get(0).getNode(),
            is(ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE)
        );
        // This next one is different. On the inference stats the node ID is a secret internal field
        // that's only used when persisting to an internal index. It's not printed as part of a REST
        // response. So there's no need to churn objects by changing this part of the response structure.
        assertThat(rolledUpStats.getInferenceStats().getNodeId(), is("node_1"));
    }

    public void testRollupAssignmentStats() {
        AssignmentStats stats = AssignmentStatsTests.randomDeploymentStats();
        AssignmentStats rolledUpStats = GetTrainedModelsStatsResponseFilter.rollupAssignmentStats(stats);
        assertThat(rolledUpStats.getNodeStats(), hasSize(1));
        assertThat(rolledUpStats.getNodeStats().get(0).getNode(), is(ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE));
    }

    public void testRollupNodeStatsGivenNoInferencesYet() {
        AssignmentStats.NodeStats stats1 = AssignmentStats.NodeStats.forStartedState(
            DiscoveryNodeUtils.create("node_1"),
            0,
            null,
            null,
            randomIntBetween(0, 100),
            0,
            0,
            randomIntBetween(0, 100),
            0,
            null,
            Instant.now(),
            4,
            5,
            0,
            0,
            null,
            0
        );
        AssignmentStats.NodeStats stats2 = AssignmentStats.NodeStats.forStartedState(
            DiscoveryNodeUtils.create("node_2"),
            0,
            null,
            null,
            randomIntBetween(0, 100),
            0,
            0,
            randomIntBetween(0, 100),
            0,
            null,
            Instant.now(),
            4,
            6,
            0,
            0,
            null,
            0
        );
        // Order shouldn't make any difference
        List<AssignmentStats.NodeStats> statsList = randomBoolean() ? List.of(stats1, stats2) : List.of(stats2, stats1);
        AssignmentStats.NodeStats rolledUp = GetTrainedModelsStatsResponseFilter.rollupNodeStats(statsList);
        assertThat(rolledUp.getNode(), is(ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE));
        assertThat(rolledUp.getInferenceCount(), is(Optional.of(0L)));
        assertThat(rolledUp.getAvgInferenceTime(), is(Optional.empty()));
        assertThat(rolledUp.getAvgInferenceTimeExcludingCacheHit(), is(Optional.empty()));
        assertThat(rolledUp.getErrorCount(), is(0));
        assertThat(rolledUp.getCacheHitCount(), is(Optional.of(0L)));
        assertThat(rolledUp.getTimeoutCount(), is(0));
        assertThat(rolledUp.getLastAccess(), nullValue());
        assertThat(rolledUp.getThreadsPerAllocation(), is(4));
        assertThat(rolledUp.getNumberOfAllocations(), is(11));
        assertThat(rolledUp.getPeakThroughput(), is(0L));
        assertThat(rolledUp.getThroughputLastPeriod(), is(0L));
        assertThat(rolledUp.getAvgInferenceTimeLastPeriod(), nullValue());
        assertThat(rolledUp.getCacheHitCountLastPeriod(), is(Optional.of(0L)));
    }

    public void testRollupNodeStatsGivenOneWorkingNodeAndOneJustStarted() {
        AssignmentStats.NodeStats stats1 = AssignmentStats.NodeStats.forStartedState(
            DiscoveryNodeUtils.create("node_1"),
            0,
            null,
            null,
            randomIntBetween(0, 100),
            0,
            0,
            randomIntBetween(0, 100),
            0,
            null,
            Instant.now(),
            4,
            5,
            0,
            0,
            null,
            0
        );
        Instant lastAccess = Instant.now();
        AssignmentStats.NodeStats stats2 = AssignmentStats.NodeStats.forStartedState(
            DiscoveryNodeUtils.create("node_2"),
            90,
            7.0,
            12.0,
            randomIntBetween(0, 100),
            1,
            45,
            randomIntBetween(0, 100),
            2,
            lastAccess,
            lastAccess.minusSeconds(100),
            4,
            6,
            7,
            5,
            10.5,
            1
        );
        // Order shouldn't make any difference
        List<AssignmentStats.NodeStats> statsList = randomBoolean() ? List.of(stats1, stats2) : List.of(stats2, stats1);
        AssignmentStats.NodeStats rolledUp = GetTrainedModelsStatsResponseFilter.rollupNodeStats(statsList);
        assertThat(rolledUp.getNode(), is(ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE));
        assertThat(rolledUp.getInferenceCount(), is(Optional.of(90L)));
        assertThat(rolledUp.getAvgInferenceTime(), is(Optional.of(7.0)));
        assertThat(rolledUp.getAvgInferenceTimeExcludingCacheHit(), is(Optional.of(12.0)));
        assertThat(rolledUp.getErrorCount(), is(1));
        assertThat(rolledUp.getCacheHitCount(), is(Optional.of(45L)));
        assertThat(rolledUp.getTimeoutCount(), is(2));
        assertThat(rolledUp.getLastAccess(), is(lastAccess));
        assertThat(rolledUp.getThreadsPerAllocation(), is(4));
        assertThat(rolledUp.getNumberOfAllocations(), is(11));
        assertThat(rolledUp.getPeakThroughput(), is(7L));
        assertThat(rolledUp.getThroughputLastPeriod(), is(5L));
        assertThat(rolledUp.getAvgInferenceTimeLastPeriod(), is(10.5));
        assertThat(rolledUp.getCacheHitCountLastPeriod(), is(Optional.of(1L)));
    }

    public void testRollupNodeStatsGivenTwoWorkingNodes() {
        Instant lastAccess1 = Instant.now();
        AssignmentStats.NodeStats stats1 = AssignmentStats.NodeStats.forStartedState(
            DiscoveryNodeUtils.create("node_1"),
            180,
            10.0,
            15.0,
            randomIntBetween(0, 100),
            5,
            45,
            randomIntBetween(0, 100),
            3,
            lastAccess1,
            lastAccess1.minusSeconds(100),
            4,
            5,
            15,
            15,
            11.5,
            2
        );
        Instant lastAccess2 = lastAccess1.plusSeconds(2);
        AssignmentStats.NodeStats stats2 = AssignmentStats.NodeStats.forStartedState(
            DiscoveryNodeUtils.create("node_2"),
            90,
            7.0,
            12.0,
            randomIntBetween(0, 100),
            1,
            45,
            randomIntBetween(0, 100),
            2,
            lastAccess2,
            lastAccess2.minusSeconds(100),
            4,
            6,
            7,
            5,
            10.5,
            1
        );
        // Order shouldn't make any difference
        List<AssignmentStats.NodeStats> statsList = randomBoolean() ? List.of(stats1, stats2) : List.of(stats2, stats1);
        AssignmentStats.NodeStats rolledUp = GetTrainedModelsStatsResponseFilter.rollupNodeStats(statsList);
        assertThat(rolledUp.getNode(), is(ServerlessMachineLearningExtension.SERVERLESS_VIRTUAL_ML_NODE));
        assertThat(rolledUp.getInferenceCount(), is(Optional.of(270L)));
        assertThat(rolledUp.getAvgInferenceTime(), is(Optional.of(9.0)));
        assertThat(rolledUp.getAvgInferenceTimeExcludingCacheHit(), is(Optional.of(14.25)));
        assertThat(rolledUp.getErrorCount(), is(6));
        assertThat(rolledUp.getCacheHitCount(), is(Optional.of(90L)));
        assertThat(rolledUp.getTimeoutCount(), is(5));
        assertThat(rolledUp.getLastAccess(), is(lastAccess2));
        assertThat(rolledUp.getThreadsPerAllocation(), is(4));
        assertThat(rolledUp.getNumberOfAllocations(), is(11));
        assertThat(rolledUp.getPeakThroughput(), is(22L));
        assertThat(rolledUp.getThroughputLastPeriod(), is(20L));
        assertThat(rolledUp.getAvgInferenceTimeLastPeriod(), is(11.25));
        assertThat(rolledUp.getCacheHitCountLastPeriod(), is(Optional.of(3L)));
    }

    public void testCombineRoutingStatesAndReasons() {
        RoutingStateAndReason x = new RoutingStateAndReason(RoutingState.STOPPED, "");
        RoutingStateAndReason y = new RoutingStateAndReason(RoutingState.STARTED, "");
        RoutingStateAndReason combined = GetTrainedModelsStatsResponseFilter.combineRoutingStatesAndReasons(x, y);
        assertThat(combined, is(y));
        x = new RoutingStateAndReason(RoutingState.STARTED, "");
        y = new RoutingStateAndReason(RoutingState.STOPPED, "");
        combined = GetTrainedModelsStatsResponseFilter.combineRoutingStatesAndReasons(x, y);
        assertThat(combined, is(x));
        x = new RoutingStateAndReason(RoutingState.FAILED, "Something went wrong");
        y = new RoutingStateAndReason(RoutingState.STOPPED, "");
        combined = GetTrainedModelsStatsResponseFilter.combineRoutingStatesAndReasons(x, y);
        assertThat(combined, is(x));
        x = new RoutingStateAndReason(RoutingState.STOPPED, "");
        y = new RoutingStateAndReason(RoutingState.FAILED, "Something went wrong");
        combined = GetTrainedModelsStatsResponseFilter.combineRoutingStatesAndReasons(x, y);
        assertThat(combined, is(y));
        x = new RoutingStateAndReason(RoutingState.FAILED, "Something went wrong");
        y = new RoutingStateAndReason(RoutingState.STARTED, "");
        combined = GetTrainedModelsStatsResponseFilter.combineRoutingStatesAndReasons(x, y);
        assertThat(combined, is(y));
        x = new RoutingStateAndReason(RoutingState.STARTED, "");
        y = new RoutingStateAndReason(RoutingState.FAILED, "Something went wrong");
        combined = GetTrainedModelsStatsResponseFilter.combineRoutingStatesAndReasons(x, y);
        assertThat(combined, is(x));
    }

    /**
     * The purpose of this test is to detect format changes to the {@link AssignmentStats.NodeStats}
     * class since this test was written. A failure of this test may indicate that the
     * {@link GetTrainedModelsStatsResponseFilter#rollupNodeStats} method needs updating. Once
     * this has been done (or confirmed to be unnecessary) this test can be made to pass again by
     * updating the {@link TransportVersion} constant used in it.
     */
    public void testNoFormatChangesSinceLastUpdate() {
        for (int runs = 0; runs < 20; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(
                randomNodeStats(DiscoveryNodeUtils.create(randomAlphaOfLength(10))),
                // If the test fails then update the constant here to the latest transport version
                // AFTER checking whether the rollupNodeStats method needs updating too.
                stats -> roundTripInstance(stats, TransportVersions.V_8_10_X),
                null
            );
        }
    }

    private AssignmentStats.NodeStats roundTripInstance(AssignmentStats.NodeStats stats, TransportVersion outStreamVersion)
        throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(outStreamVersion);
            stats.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                in.setTransportVersion(TransportVersion.current());
                return new AssignmentStats.NodeStats(in);
            }
        }
    }
}
