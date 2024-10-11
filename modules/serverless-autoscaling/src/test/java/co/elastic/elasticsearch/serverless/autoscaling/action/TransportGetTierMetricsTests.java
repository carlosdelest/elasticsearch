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

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.Mockito.when;

public class TransportGetTierMetricsTests extends ESTestCase {

    private ClusterService clusterService;
    private TransportService transportService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        var threadPool = Mockito.mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        clusterService = createClusterService(
            threadPool,
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    TransportGetAutoscalingMetricsAction.AUTOSCALING_METRICS_ENABLED_SETTING
                )
            )
        );
        transportService = new CapturingTransport().createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        ActionFilters actionFilters = new ActionFilters(Set.of());

        new TransportGetAutoscalingMetricsAction(transportService, clusterService, threadPool, actionFilters, null, null);
        new TransportGetMachineLearningTierMetrics(transportService, actionFilters, clusterService, null);
        new TransportGetSearchTierMetrics(transportService, clusterService, threadPool, actionFilters, null, null);
        new TransportGetIndexTierMetrics(transportService, clusterService, threadPool, actionFilters, null, null);
    }

    public void testCanTripCircuitBreaker() {
        assertFalse(
            transportService.getRequestHandler("cluster:admin/serverless/autoscaling/get_serverless_autoscaling_metrics")
                .canTripCircuitBreaker()
        );
        assertFalse(
            transportService.getRequestHandler("cluster:internal/serverless/autoscaling/get_serverless_ml_tier_metrics")
                .canTripCircuitBreaker()
        );
        assertFalse(
            transportService.getRequestHandler("cluster:internal/serverless/autoscaling/get_serverless_search_tier_metrics")
                .canTripCircuitBreaker()
        );
        assertFalse(
            transportService.getRequestHandler("cluster:internal/serverless/autoscaling/get_serverless_index_tier_metrics")
                .canTripCircuitBreaker()
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }
}
