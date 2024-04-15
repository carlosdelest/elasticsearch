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

package co.elastic.elasticsearch.serverless.shutdown;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ShutdownPersistentTasksStatus;
import org.elasticsearch.cluster.metadata.ShutdownPluginsStatus;
import org.elasticsearch.cluster.metadata.ShutdownShardMigrationStatus;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.SingleNodeShutdownStatus;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SigtermTerminationHandlerTests extends ESTestCase {

    @TestLogging(
        reason = "Testing logging at INFO level",
        value = "co.elastic.elasticsearch.serverless.shutdown.SigtermTerminationHandler:INFO"
    )
    public void testShutdownCompletesImmediate() {
        final TimeValue pollInterval = TimeValue.parseTimeValue(randomPositiveTimeValue(), this.getTestName());
        final TimeValue timeout = TimeValue.parseTimeValue(randomPositiveTimeValue(), this.getTestName());
        final String nodeId = randomAlphaOfLength(10);
        TestThreadPool threadPool = new TestThreadPool(this.getTestName());

        final var appender = new MockLogAppender();
        try (var ignored = appender.capturing(SigtermTerminationHandler.class)) {
            Client client = mock(Client.class);
            when(client.threadPool()).thenReturn(threadPool);
            doAnswer(invocation -> {
                PutShutdownNodeAction.Request putRequest = invocation.getArgument(1, PutShutdownNodeAction.Request.class);
                assertEquals(timeout, putRequest.ackTimeout());
                assertEquals(timeout, putRequest.timeout());
                assertEquals(timeout, putRequest.masterNodeTimeout());
                assertThat(putRequest.getNodeId(), equalTo(nodeId));
                assertThat(putRequest.getType(), equalTo(SingleNodeShutdownMetadata.Type.SIGTERM));
                ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
                listener.onResponse(AcknowledgedResponse.TRUE);
                return null; // real method is void
            }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

            doAnswer(invocation -> {
                GetShutdownStatusAction.Request getRequest = invocation.getArgument(1, GetShutdownStatusAction.Request.class);
                assertThat(getRequest.getNodeIds()[0], equalTo(nodeId));
                assertEquals(timeout, getRequest.masterNodeTimeout());
                ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
                listener.onResponse(
                    new GetShutdownStatusAction.Response(
                        Collections.singletonList(
                            new SingleNodeShutdownStatus(
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                    .setReason(this.getTestName())
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setGracePeriod(timeout)
                                    .build(),
                                new ShutdownShardMigrationStatus(SingleNodeShutdownMetadata.Status.COMPLETE, 0, 0, 0),
                                new ShutdownPersistentTasksStatus(),
                                new ShutdownPluginsStatus(true)
                            )
                        )
                    )
                );
                return null; // real method is void
            }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "handler started message",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "handling graceful shutdown request"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "handler completed message",
                    SigtermTerminationHandler.class.getCanonicalName(),
                    Level.INFO,
                    "shutdown completed"
                )
            );

            new SigtermTerminationHandler(client, threadPool, pollInterval, timeout, nodeId).handleTermination();

            appender.assertAllExpectationsMatched();

            verify(client, times(1)).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());
            verify(client, times(1)).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());
        } finally {
            threadPool.shutdownNow();
        }
    }

    public void testShutdownStalledRequiresPolling() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.STALLED, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownRequestPollingThenCompletes() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.IN_PROGRESS, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownRequestPollingThenStalls() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.IN_PROGRESS, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void testShutdownNotStartedThenCompletes() {
        shutdownRequiresPolling(SingleNodeShutdownMetadata.Status.NOT_STARTED, SingleNodeShutdownMetadata.Status.COMPLETE);
    }

    public void shutdownRequiresPolling(
        SingleNodeShutdownMetadata.Status incompleteStatus,
        SingleNodeShutdownMetadata.Status finishedStatus
    ) {
        final TimeValue pollInterval = TimeValue.timeValueMillis(5);
        // Should be way more than enough to allow rounds of iteration but not block tests forever if something breaks
        final TimeValue timeout = TimeValue.timeValueSeconds(10);
        final String nodeId = randomAlphaOfLength(10);

        int initialRounds = randomIntBetween(2, 5);
        AtomicInteger rounds = new AtomicInteger(initialRounds);

        TestThreadPool threadPool = new TestThreadPool(this.getTestName());
        try {
            Client client = mock(Client.class);
            when(client.threadPool()).thenReturn(threadPool);
            doAnswer(invocation -> {
                PutShutdownNodeAction.Request putRequest = invocation.getArgument(1, PutShutdownNodeAction.Request.class);
                assertThat(putRequest.getNodeId(), equalTo(nodeId));
                assertThat(putRequest.getType(), equalTo(SingleNodeShutdownMetadata.Type.SIGTERM));
                ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
                listener.onResponse(randomFrom(AcknowledgedResponse.TRUE, AcknowledgedResponse.FALSE));
                return null; // real method is void
            }).when(client).execute(eq(PutShutdownNodeAction.INSTANCE), any(), any());

            doAnswer(invocation -> {
                int thisRound = rounds.decrementAndGet();
                assertThat(thisRound, greaterThanOrEqualTo(0));
                SingleNodeShutdownMetadata.Status status = thisRound == 0 ? finishedStatus : incompleteStatus;
                GetShutdownStatusAction.Request getRequest = invocation.getArgument(1, GetShutdownStatusAction.Request.class);
                assertThat(getRequest.getNodeIds()[0], equalTo(nodeId));
                ActionListener<GetShutdownStatusAction.Response> listener = invocation.getArgument(2);
                listener.onResponse(
                    new GetShutdownStatusAction.Response(
                        Collections.singletonList(
                            new SingleNodeShutdownStatus(
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId)
                                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                                    .setReason(this.getTestName())
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setGracePeriod(timeout)
                                    .build(),
                                new ShutdownShardMigrationStatus(status, 0, 0, 0),
                                new ShutdownPersistentTasksStatus(),
                                new ShutdownPluginsStatus(true)
                            )
                        )
                    )
                );
                return null; // real method is void
            }).when(client).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());

            SigtermTerminationHandler handler = new SigtermTerminationHandler(client, threadPool, pollInterval, timeout, nodeId);
            handler.handleTermination();

            verify(client, times(initialRounds)).execute(eq(GetShutdownStatusAction.INSTANCE), any(), any());
        } finally {
            threadPool.shutdownNow();
        }
    }

}
