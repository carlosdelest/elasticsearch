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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import co.elastic.elasticsearch.serverless.autoscaling.MachineLearningTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IndexTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.search.SearchTierMetrics;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class TransportGetAutoscalingMetricsActionTests extends ESTestCase {

    public void testExecuteRequestSuccess() throws InterruptedException {
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);
        var countDownListener = new CountDownActionListener(1, ActionListener.wrap(r -> latch.countDown(), exceptionRef::set));

        final Client client = new NoOpClient(threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) new GetSearchTierMetrics.Response(new SearchTierMetrics(null, null, null, List.of())));
            }
        };

        TransportGetAutoscalingMetricsAction.executeRequest(
            client,
            threadPool,
            GetSearchTierMetrics.INSTANCE,
            new GetSearchTierMetrics.Request(TimeValue.THIRTY_SECONDS, TimeValue.timeValueSeconds(10)),
            ActionListener.wrap(response -> searchTierMetricsRef.set(response.getMetrics()), exceptionRef::set),
            countDownListener
        );

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue("did not complete", latch.await(0, TimeUnit.SECONDS));
        assertNull(exceptionRef.get());
        assertNotNull(searchTierMetricsRef.get());
    }

    public void testExecuteRequestFailure() throws InterruptedException {
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);
        var countDownListener = new CountDownActionListener(1, ActionListener.wrap(r -> latch.countDown(), exceptionRef::set));

        final Client client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onFailure(new ElasticsearchException("internal error"));
            }
        };

        TransportGetAutoscalingMetricsAction.executeRequest(
            client,
            threadPool,
            GetSearchTierMetrics.INSTANCE,
            new GetSearchTierMetrics.Request(TimeValue.THIRTY_SECONDS, TimeValue.timeValueMillis(10)),
            ActionListener.wrap(response -> searchTierMetricsRef.set(response.getMetrics()), exceptionRef::set),
            countDownListener
        );

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue("did not complete", latch.await(0, TimeUnit.SECONDS));
        assertTrue(exceptionRef.get() instanceof ElasticsearchException);
        assertNull(searchTierMetricsRef.get());
    }

    public void testExecuteRequestTimeOut() throws InterruptedException {
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        var countDownListener = new CountDownActionListener(
            1,
            ActionListener.wrap(ignored -> latch.countDown(), e -> fail("received an exception: " + e.getMessage()))
        );

        final Client client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {}
        };
        TransportGetAutoscalingMetricsAction.executeRequest(
            client,
            threadPool,
            GetSearchTierMetrics.INSTANCE,
            new GetSearchTierMetrics.Request(TimeValue.THIRTY_SECONDS, TimeValue.timeValueMillis(10)),
            ActionListener.wrap(r -> searchTierMetricsRef.set(r.getMetrics()), exceptionRef::set),
            countDownListener
        );

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue("did not complete", latch.await(0, TimeUnit.SECONDS));
        assertTrue(exceptionRef.get() instanceof ElasticsearchTimeoutException);
        assertNull(searchTierMetricsRef.get());
    }

    @FixForMultiProject(description = "Remove once ML autoscaling metrics work. See also https://elasticco.atlassian.net/browse/ES-10838")
    public void testSkipMLAutoscalingMetricsForMultiProjects() {
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final ClusterService clusterService = createClusterService(
            threadPool,
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    TransportGetAutoscalingMetricsAction.AUTOSCALING_METRICS_ENABLED_SETTING
                )
            )
        );

        final SearchTierMetrics searchTierMetrics = mock(SearchTierMetrics.class);
        final IndexTierMetrics indexTierMetrics = mock(IndexTierMetrics.class);
        final MachineLearningTierMetrics machineLearningTierMetrics = mock(MachineLearningTierMetrics.class);

        final Client client = new NoOpClient(threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (GetMachineLearningTierMetrics.NAME.equals(action.name())) {
                    listener.onResponse((Response) new GetMachineLearningTierMetrics.Response(machineLearningTierMetrics));
                } else if (GetSearchTierMetrics.NAME.equals(action.name())) {
                    listener.onResponse((Response) new GetSearchTierMetrics.Response(searchTierMetrics));
                } else if (GetIndexTierMetrics.NAME.equals(action.name())) {
                    listener.onResponse((Response) new GetIndexTierMetrics.Response(indexTierMetrics));
                } else {
                    fail("unexpected action: " + action.name());
                }
            }
        };

        final AtomicBoolean supportsMultipleProjects = new AtomicBoolean(true);
        final ProjectResolver projectResolver = new ProjectResolver() {
            @Override
            public boolean supportsMultipleProjects() {
                return supportsMultipleProjects.get();
            }

            @Override
            public ProjectId getProjectId() {
                return null;
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {}
        };

        final var action = new TransportGetAutoscalingMetricsAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            client,
            projectResolver
        );

        final Task task = new Task(randomNonNegativeLong(), randomAlphaOfLength(5), GetAutoscalingMetricsAction.NAME, "", null, Map.of());
        // Skip ML autoscaling metrics when multi-projects is enabled
        try (var mockLog = MockLog.capture(TransportGetAutoscalingMetricsAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "skip ML autoscaling metrics",
                    TransportGetAutoscalingMetricsAction.class.getName(),
                    Level.INFO,
                    "multi-project is enabled, skip retrieving ml tier metrics"
                )
            );
            final PlainActionFuture<GetAutoscalingMetricsAction.Response> future = new PlainActionFuture<>();
            action.masterOperation(
                task,
                new GetAutoscalingMetricsAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT),
                clusterService.state(),
                future
            );
            deterministicTaskQueue.runAllTasksInTimeOrder();
            final GetAutoscalingMetricsAction.Response response = safeGet(future);
            assertThat(response.getIndexTierMetrics(), sameInstance(indexTierMetrics));
            assertThat(response.getSearchTierMetrics(), sameInstance(searchTierMetrics));
            assertThat(response.getMachineLearningTierMetrics(), nullValue());
            mockLog.assertAllExpectationsMatched();
        }

        // Not skip ML autoscaling metrics when multi-projects is disabled
        supportsMultipleProjects.set(false);
        {
            final PlainActionFuture<GetAutoscalingMetricsAction.Response> future = new PlainActionFuture<>();
            action.masterOperation(
                task,
                new GetAutoscalingMetricsAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT),
                clusterService.state(),
                future
            );
            deterministicTaskQueue.runAllTasksInTimeOrder();
            final GetAutoscalingMetricsAction.Response response = safeGet(future);
            assertThat(response.getIndexTierMetrics(), sameInstance(indexTierMetrics));
            assertThat(response.getSearchTierMetrics(), sameInstance(searchTierMetrics));
            assertThat(response.getMachineLearningTierMetrics(), sameInstance(machineLearningTierMetrics));
        }
    }
}
