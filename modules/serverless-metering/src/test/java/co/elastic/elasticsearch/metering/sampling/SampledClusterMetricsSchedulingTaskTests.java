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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.quality.Strictness;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SampledClusterMetricsSchedulingTaskTests extends ESTestCase {

    private void mockSampleUpdate(SampledClusterMetricsService mock, boolean success) {
        doAnswer(answer -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<Void>) answer.getArgument(1, ActionListener.class);
            if (success) {
                listener.onResponse(null);
            } else {
                listener.onFailure(new RuntimeException("Sample update failed"));
            }
            return null;
        }).when(mock).updateSamples(any(), any());
    }

    public void testRunInvokesService() {
        var mockThreadPool = mock(ThreadPool.class, withSettings().strictness(Strictness.STRICT_STUBS));
        var mockSampleService = mock(SampledClusterMetricsService.class);
        var mockScheduler = mock(ScheduledExecutorService.class);

        var initialPollInterval = TimeValue.timeValueMinutes(12);

        var task = new SampledClusterMetricsSchedulingTask(
            0L,
            "",
            "",
            "",
            null,
            null,
            mockThreadPool,
            mockSampleService,
            mock(Client.class),
            () -> initialPollInterval,
            () -> {}
        );

        when(mockThreadPool.scheduler()).thenReturn(mockScheduler);
        when(mockThreadPool.schedule(any(), any(), any())).then(invocationOnMock -> createScheduled(invocationOnMock.getArgument(1)));
        mockSampleUpdate(mockSampleService, randomBoolean());

        task.run();

        assertThat(task.scheduled, notNullValue());
        assertThat(task.scheduled.isCancelled(), is(false));
        assertThat(task.scheduled.getDelay(TimeUnit.MINUTES), is(12L));

        verify(mockSampleService, times(1)).updateSamples(any(), any());
    }

    public void testCancellationStopsServiceInvocation() {
        var mockThreadPool = mock(ThreadPool.class, withSettings().strictness(Strictness.STRICT_STUBS));
        var mockSampleService = mock(SampledClusterMetricsService.class);
        var mockScheduler = mock(ScheduledExecutorService.class);

        var initialPollInterval = TimeValue.timeValueMinutes(12);

        var task = spy(
            new SampledClusterMetricsSchedulingTask(
                0L,
                "",
                "",
                "",
                null,
                null,
                mockThreadPool,
                mockSampleService,
                mock(Client.class),
                () -> initialPollInterval,
                () -> {}
            )
        );

        when(mockThreadPool.scheduler()).thenReturn(mockScheduler);
        when(mockThreadPool.schedule(any(), any(), any())).then(invocationOnMock -> createScheduled(invocationOnMock.getArgument(1)));
        doNothing().when(task).markAsCompleted();
        mockSampleUpdate(mockSampleService, randomBoolean());

        task.run();
        verify(mockSampleService, times(1)).updateSamples(any(), any());

        assertThat(task.scheduled, notNullValue());
        assertThat(task.scheduled.isCancelled(), is(false));
        assertThat(task.scheduled.getDelay(TimeUnit.MINUTES), is(12L));

        TaskCancelHelper.cancel(task, "Cancelled");
        assertThat(task.scheduled.isCancelled(), is(true));

        // Run on a cancelled task no longer invokes the service
        task.run();
        verify(mockSampleService, times(1)).updateSamples(any(), any());
    }

    public void testRequestRescheduleCancelsAndRescheduleImmediately() {
        var mockThreadPool = mock(ThreadPool.class, withSettings().strictness(Strictness.STRICT_STUBS));
        var mockSampleService = mock(SampledClusterMetricsService.class, withSettings().strictness(Strictness.STRICT_STUBS));
        var mockScheduler = mock(ScheduledExecutorService.class);

        var initialPollInterval = TimeValue.timeValueMinutes(12);

        var task = new SampledClusterMetricsSchedulingTask(
            0L,
            "",
            "",
            "",
            null,
            null,
            mockThreadPool,
            mockSampleService,
            mock(Client.class),
            () -> initialPollInterval,
            () -> {}
        );

        when(mockThreadPool.scheduler()).thenReturn(mockScheduler);
        when(mockThreadPool.schedule(any(), any(), any())).then(invocationOnMock -> createScheduled(invocationOnMock.getArgument(1)));
        mockSampleUpdate(mockSampleService, randomBoolean());

        task.run();

        assertThat(task.scheduled, notNullValue());
        assertThat(task.scheduled.isCancelled(), is(false));
        assertThat(task.scheduled.getDelay(TimeUnit.MINUTES), is(12L));
        var firstScheduled = task.scheduled;

        task.requestReschedule();
        assertThat(task.scheduled, notNullValue());
        assertThat(task.scheduled, not(firstScheduled));
        assertThat(task.scheduled.isCancelled(), is(false));
        assertThat(task.scheduled.getDelay(TimeUnit.MINUTES), is(0L));
        assertThat(firstScheduled.isCancelled(), is(true));
    }

    private static Scheduler.ScheduledCancellable createScheduled(TimeValue argument) {
        return new Scheduler.ScheduledCancellable() {
            private volatile boolean cancelled = false;

            @Override
            public long getDelay(TimeUnit unit) {
                return unit.convert(argument.duration(), argument.timeUnit());
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }

            @Override
            public boolean cancel() {
                cancelled = true;
                return cancelled;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }
        };
    }
}
