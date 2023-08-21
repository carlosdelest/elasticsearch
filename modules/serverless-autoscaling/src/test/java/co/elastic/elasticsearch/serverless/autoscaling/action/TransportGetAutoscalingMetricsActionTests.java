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

import co.elastic.elasticsearch.stateless.autoscaling.search.SearchTierMetrics;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TransportGetAutoscalingMetricsActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(ThreadPool.Names.GENERIC);
    }

    @After
    public void tearDownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testExecuteRequestSuccess() throws InterruptedException {
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);
        var countDownListener = new CountDownActionListener(1, ActionListener.wrap(r -> latch.countDown(), exceptionRef::set));

        try (Client client = new NoOpClient(getTestName()) {
            @SuppressWarnings("unchecked")
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) new GetSearchTierMetrics.Response(new SearchTierMetrics(null, null, null)));
            }
        }) {
            TransportGetAutoscalingMetricsAction.executeRequest(
                client,
                threadPool,
                GetSearchTierMetrics.INSTANCE,
                new GetSearchTierMetrics.Request(TimeValue.timeValueSeconds(10)),
                ActionListener.wrap(response -> searchTierMetricsRef.set(response.getMetrics()), exceptionRef::set),
                countDownListener
            );
        }

        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
        assertNull(exceptionRef.get());
        assertNotNull(searchTierMetricsRef.get());
    }

    public void testExecuteRequestFailure() throws InterruptedException {
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);
        var countDownListener = new CountDownActionListener(1, ActionListener.wrap(r -> latch.countDown(), exceptionRef::set));

        try (Client client = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onFailure(new ElasticsearchException("internal error"));
            }
        }) {
            TransportGetAutoscalingMetricsAction.executeRequest(
                client,
                threadPool,
                GetSearchTierMetrics.INSTANCE,
                new GetSearchTierMetrics.Request(TimeValue.timeValueMillis(10)),
                ActionListener.wrap(response -> searchTierMetricsRef.set(response.getMetrics()), exceptionRef::set),
                countDownListener
            );
        }

        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
        assertTrue(exceptionRef.get() instanceof ElasticsearchException);
        assertNull(searchTierMetricsRef.get());
    }

    public void testExecuteRequestTimeOut() throws InterruptedException {
        final AtomicReference<SearchTierMetrics> searchTierMetricsRef = new AtomicReference<>();
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        var countDownListener = new CountDownActionListener(
            1,
            ActionListener.wrap(ignored -> latch.countDown(), e -> fail("received an exception: " + e.getMessage()))
        );

        try (Client client = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {}
        }) {
            TransportGetAutoscalingMetricsAction.executeRequest(
                client,
                threadPool,
                GetSearchTierMetrics.INSTANCE,
                new GetSearchTierMetrics.Request(TimeValue.timeValueMillis(10)),
                ActionListener.wrap(r -> searchTierMetricsRef.set(r.getMetrics()), exceptionRef::set),
                countDownListener
            );

            assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
            assertTrue(exceptionRef.get() instanceof ElasticsearchTimeoutException);
            assertNull(searchTierMetricsRef.get());
        }
    }
}
