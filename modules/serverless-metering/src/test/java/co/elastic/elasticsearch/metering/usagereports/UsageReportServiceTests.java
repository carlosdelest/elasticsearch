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

import co.elastic.elasticsearch.metering.usagereports.SampledMetricsTimeCursor.Timestamps;

import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class UsageReportServiceTests extends ESTestCase {

    public void testStartAndStopCollector() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        UsageReportService service = new UsageReportService(
            "nodeId",
            "projectId",
            List.of(),
            List.of(),
            mock(SampledMetricsTimeCursor.class, v -> Timestamps.EMPTY),
            TimeValue.timeValueSeconds(5),
            mock(),
            deterministicTaskQueue.getThreadPool(),
            deterministicTaskQueue.getThreadPool().generic(),
            mock(Clock.class, x -> Instant.ofEpochMilli(deterministicTaskQueue.getCurrentTimeMillis())),
            MeterRegistry.NOOP
        );

        service.start();

        assertThat(
            "A first gatherReports task should have been scheduled",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(true))).and(
                transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(false))
            )
        );

        deterministicTaskQueue.advanceTime();

        assertThat(
            "There should be a gatherReports task ready to run",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(true))).and(
                transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(false))
            )
        );

        deterministicTaskQueue.runRandomTask();

        assertThat(
            "A second gatherReports task should have been scheduled",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(true))).and(
                transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(false))
            )
        );

        service.stop();

        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runRandomTask();

        assertThat(
            "There should be no gatherReports task scheduled",
            deterministicTaskQueue,
            both(transformedMatch(DeterministicTaskQueue::hasDeferredTasks, is(false))).and(
                transformedMatch(DeterministicTaskQueue::hasRunnableTasks, is(false))
            )
        );
    }
}
