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

package co.elastic.elasticsearch.metering.usagereports.action;

import co.elastic.elasticsearch.metering.usagereports.action.SampledMetricsMetadataService.UpsertSampledMetricsMetadataTask;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Instant;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SampledMetricsMetadataServiceTests extends ESTestCase {

    private SampledMetricsMetadataService createService(MasterServiceTaskQueue<UpsertSampledMetricsMetadataTask> taskQueue) {
        ClusterService clusterService = mock();
        Mockito.when(clusterService.createTaskQueue(any(), any(), any(SampledMetricsMetadataService.Executor.class))).thenReturn(taskQueue);
        return new SampledMetricsMetadataService(clusterService);
    }

    public void testSampledMetricsMetadataUpdate() throws Exception {
        ActionListener<Void> listener = mock();
        MasterServiceTaskQueue<UpsertSampledMetricsMetadataTask> taskQueue = mock();
        SampledMetricsMetadataService service = createService(taskQueue);

        Instant now = Instant.now().truncatedTo(MINUTES);

        service.update(new SampledMetricsMetadata(now.minus(1, MINUTES)), TimeValue.ZERO, listener);
        service.update(new SampledMetricsMetadata(now.minus(1, MINUTES)), TimeValue.ZERO, listener);
        service.update(new SampledMetricsMetadata(now), TimeValue.ZERO, listener);

        Mockito.verifyNoInteractions(listener);

        ArgumentCaptor<UpsertSampledMetricsMetadataTask> captor = ArgumentCaptor.captor();
        verify(taskQueue, times(3)).submitTask(any(), captor.capture(), Mockito.eq(TimeValue.ZERO));

        ClusterState finalState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            ClusterState.EMPTY_STATE,
            new SampledMetricsMetadataService.Executor(),
            captor.getAllValues()
        );

        assertThat(SampledMetricsMetadata.getFromClusterState(finalState), equalTo(new SampledMetricsMetadata(now)));
        verify(listener, times(3)).onResponse(null);
    }
}
