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

package co.elastic.elasticsearch.stateless.cluster;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.memory.PublishHeapMemoryMetricsRequest;
import co.elastic.elasticsearch.stateless.autoscaling.memory.TransportPublishHeapMemoryMetrics;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.ShardHeapUsage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StatelessClusterInfoServiceIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.frequency",
            TimeValue.timeValueMillis(10)
        );
    }

    public void testClusterInfoIncludesHeapUsage() throws Exception {
        startMasterAndIndexNode();
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(3);
        final MockTransportService masterMockTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        final CountDownLatch heapMetricsReceived = new CountDownLatch(1);
        masterMockTransportService.addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, (handler, request, channel, task) -> {
            handler.messageReceived(request, channel, task);
            if (asInstanceOf(PublishHeapMemoryMetricsRequest.class, request).getHeapMemoryUsage().shardMappingSizes().size() > 0) {
                heapMetricsReceived.countDown();
            }
        });

        final String indexName = randomIdentifier();
        indexDocsAndRefresh(indexName, randomInt(1_000));
        ensureGreen(indexName);

        safeAwait(heapMetricsReceived);

        final InternalClusterInfoService infoService = asInstanceOf(
            InternalClusterInfoService.class,
            internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class)
        );

        ClusterInfoServiceUtils.setUpdateFrequency(infoService, TimeValue.timeValueMillis(100));
        final ClusterInfo info = ClusterInfoServiceUtils.refresh(infoService);
        final Map<String, ShardHeapUsage> nodesHeapUsage = info.getShardHeapUsages();
        assertThat(nodesHeapUsage.size(), greaterThan(0));
    }
}
