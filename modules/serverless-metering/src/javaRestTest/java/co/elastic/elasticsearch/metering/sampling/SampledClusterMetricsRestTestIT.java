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

import co.elastic.elasticsearch.metering.AbstractMeteringRestTestIT;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.cluster.serverless.local.DefaultServerlessLocalConfigProvider.node;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SampledClusterMetricsRestTestIT extends AbstractMeteringRestTestIT {
    @Override
    protected void configureNodes(AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> builder) {
        builder.withNode(node("search2", "search"));
    }

    @Override
    protected void configureCluster(AbstractLocalClusterSpecBuilder<ServerlessElasticsearchCluster> builder) {
        super.configureCluster(builder);
        builder.setting("logger.level", "ERROR").setting("logger.co.elastic.elasticsearch.metering", "INFO");
    }

    private Set<String> latestSamplingNodeIds() {
        return drainAllUsageRecords().stream()
            .map(m -> (String) ((Map<?, ?>) m.get("source")).get("id"))
            .map(id -> id.startsWith("es-") ? id.substring(3) : id)
            .collect(Collectors.toSet());
    }

    private void restartClusterAndResetClients(boolean forcibly) throws IOException {
        cluster.restart(forcibly);
        closeClients();
        initClient();
    }

    public void testSamplingTaskAssignmentDuringClusterRestart() throws Exception {
        Set<String> previousNodeIds = new HashSet<>();

        assertBusy(() -> {
            Set<String> nodeIds = latestSamplingNodeIds();
            assertThat(nodeIds, hasSize(1)); // wait until we see a single persistent task node only
            previousNodeIds.addAll(nodeIds);
        });

        restartClusterAndResetClients(false);

        assertBusy(() -> {
            Set<String> nodeIds = latestSamplingNodeIds();
            assertThat(nodeIds, hasSize(1));

            assertThat(previousNodeIds, not(hasItem(nodeIds.iterator().next()))); // new node id
            previousNodeIds.addAll(nodeIds);
        });

        restartClusterAndResetClients(true);

        assertBusy(() -> {
            Set<String> nodeIds = latestSamplingNodeIds();
            assertThat(nodeIds, hasSize(1));
            assertThat(previousNodeIds, not(hasItem(nodeIds.iterator().next()))); // new node id
            previousNodeIds.addAll(nodeIds);
        });
    }
}
