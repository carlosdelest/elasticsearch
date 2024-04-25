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

package co.elastic.elasticsearch.serverless.health;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessShardsHealthIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new HashSet<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(ServerlessShardsHealthPlugin.class);
        return plugins;
    }

    public void testReplicaShardsAvailabilityOnServerless() throws Exception {
        startMasterAndIndexNode();

        String indexName = "myindex";
        createIndex(indexName, 2, 0);
        ensureGreen(indexName);

        waitForStatusAndGet(HealthStatus.GREEN);

        // Increase the number of replicas to 2, so that the index goes red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.number_of_replicas", 2))
            .get();

        GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();

        HealthIndicatorResult hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.RED));
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Not all data is searchable. No searchable copies of the data exist on 1 index [myindex].")
        );
        assertThat(hir.symptom(), containsString("This cluster has 4 unavailable replica shards."));
        assertThat(hir.diagnosisList().stream().map(d -> d.definition().id()).toList(), equalTo(List.of("debug_node:role:search")));
        assertThat(
            hir.diagnosisList()
                .stream()
                .flatMap(
                    d -> d.affectedResources()
                        .stream()
                        .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                        .flatMap(r -> r.getValues().stream())
                )
                .toList(),
            equalTo(List.of(indexName))
        );

        // Start a search node so that one of the replicas can be allocated
        startSearchNode();

        hir = waitForStatusAndGet(HealthStatus.YELLOW);
        // We should inherit the existing impacts from the Stateful code version
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Searches might be slower than usual. Fewer redundant copies of the data exist on 1 index [myindex].")
        );
        assertThat(hir.symptom(), containsString("This cluster has 2 unavailable replica shards."));
        assertThat(hir.diagnosisList().stream().map(d -> d.definition().id()).toList(), equalTo(List.of("update_shards:role:search")));
        assertThat(
            hir.diagnosisList()
                .stream()
                .flatMap(
                    d -> d.affectedResources()
                        .stream()
                        .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                        .flatMap(r -> r.getValues().stream())
                )
                .toList(),
            equalTo(List.of(indexName))
        );

        // Start one more search node so everything can go green
        startSearchNode();
        waitForStatusAndGet(HealthStatus.GREEN);
    }

    public void testReplicaShardsForNewPrimariesAvailabilityOnServerless() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        // Shard health treats all replicas allocated as a "everything is green" sort of situation.
        // However, the replicas.areAllAvailable() correctly ignores replicas that are initializing.
        // We need it to look further in more detail, so we need to force there to be at least one
        // unassigned replica. We create an index with two replicas (there is only one search node)
        // for this purpose.
        createIndex("other", 1, 2);

        // We hook in to the transport service and catch the "shard started" event for the
        // soon-to-be-created "myindex" index. We want to delay it so the primary stays in the
        // "INITIALIZING" state until we release it.
        var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());
        CountDownLatch latch = new CountDownLatch(1);
        masterTransportService.<ShardStateAction.StartedShardEntry>addRequestHandlingBehavior(
            ShardStateAction.SHARD_STARTED_ACTION_NAME,
            (handler, request, channel, task) -> {
                if (request.toString().contains("[myindex]")) {
                    latch.await(30, TimeUnit.SECONDS);
                }
                handler.messageReceived(request, channel, task);
            }
        );

        // Create the index (don't bother waiting for active shards because we've delayed them
        // until release the latch.
        String indexName = "myindex";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1))
            .setWaitForActiveShards(0)
            .get();

        // The health should be yellow instead of red, even though the replica is not allocated,
        // because we treat replicas for "INITIALIZING" primaries as "special"
        assertBusy(() -> {
            GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
            HealthIndicatorResult hir = health.findIndicator("shards_availability");
            assertThat(hir.status(), equalTo(HealthStatus.YELLOW));
        });

        // Release the latch so the primary can move from INITIALIZING -> STARTED
        latch.countDown();
        // We should go green for the "myindex" index finally.
        ensureGreen(indexName);
    }

    public void testIncreaseClusterShardLimit() throws Exception {
        startMasterAndIndexNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchNode = startSearchNode();

        // With this variable we choose if we want this experiment to affect the index or the search nodes
        boolean shutDownIndexNode = randomBoolean();

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1))
            .get();

        String indexName = "myindex";
        createIndex(indexName, indexSettings(2, 1).put("index.unassigned.node_left.delayed_timeout", "0ms").build());
        waitForStatusAndGet(HealthStatus.GREEN);

        // Stop a node
        internalCluster().stopNode(shutDownIndexNode ? indexNode : searchNode);

        HealthIndicatorResult hir = waitForStatusAndGet(HealthStatus.RED);
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString(
                shutDownIndexNode
                    ? "Cannot add data to 1 index [myindex]. Searches might return incomplete results."
                    : "Not all data is searchable. No searchable copies of the data exist on 1 index [myindex]."
            )
        );
        assertThat(
            hir.symptom(),
            containsString(
                shutDownIndexNode
                    ? "This cluster has 1 unavailable primary shard, 1 unavailable replica shard."
                    : "This cluster has 1 unavailable replica shard."
            )
        );
        assertThat(hir.diagnosisList().size(), equalTo(shutDownIndexNode ? 2 : 1));
        Diagnosis diagnosis = hir.diagnosisList().get(0);
        assertThat(
            diagnosis.definition().id(),
            equalTo("increase_shard_limit_cluster_setting:role:" + (shutDownIndexNode ? "index" : "search"))
        );
        assertThat(
            diagnosis.affectedResources()
                .stream()
                .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                .flatMap(r -> r.getValues().stream())
                .toList(),
            equalTo(List.of(indexName))
        );

        if (shutDownIndexNode) {
            diagnosis = hir.diagnosisList().get(1);
            assertThat(diagnosis.definition().id(), equalTo("explain_allocations"));
            assertThat(
                diagnosis.affectedResources()
                    .stream()
                    .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                    .flatMap(r -> r.getValues().stream())
                    .toList(),
                equalTo(List.of(indexName))
            );
        }
    }

    public void testIncreaseIndexShardLimit() throws Exception {
        startMasterAndIndexNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchNode = startSearchNode();

        // With this variable we choose if we want this experiment to affect the index or the search nodes
        boolean shutDownIndexNode = randomBoolean();

        String indexName = "myindex";
        createIndex(
            indexName,
            indexSettings(2, 1).put(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                .put("index.unassigned.node_left.delayed_timeout", "0ms")
                .build()
        );
        waitForStatusAndGet(HealthStatus.GREEN);

        // Stop a node
        internalCluster().stopNode(shutDownIndexNode ? indexNode : searchNode);

        HealthIndicatorResult hir = waitForStatusAndGet(HealthStatus.RED);
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString(
                shutDownIndexNode
                    ? "Cannot add data to 1 index [myindex]. Searches might return incomplete results."
                    : "Not all data is searchable. No searchable copies of the data exist on 1 index [myindex]."
            )
        );
        assertThat(
            hir.symptom(),
            containsString(
                shutDownIndexNode
                    ? "This cluster has 1 unavailable primary shard, 1 unavailable replica shard."
                    : "This cluster has 1 unavailable replica shard."
            )
        );
        assertThat(hir.diagnosisList().size(), equalTo(shutDownIndexNode ? 2 : 1));
        Diagnosis diagnosis = hir.diagnosisList().get(0);
        assertThat(
            diagnosis.definition().id(),
            equalTo("increase_shard_limit_index_setting:role:" + (shutDownIndexNode ? "index" : "search"))
        );
        assertThat(
            diagnosis.affectedResources()
                .stream()
                .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                .flatMap(r -> r.getValues().stream())
                .toList(),
            equalTo(List.of(indexName))
        );

        if (shutDownIndexNode) {
            diagnosis = hir.diagnosisList().get(1);
            assertThat(diagnosis.definition().id(), equalTo("explain_allocations"));
            assertThat(
                diagnosis.affectedResources()
                    .stream()
                    .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                    .flatMap(r -> r.getValues().stream())
                    .toList(),
                equalTo(List.of(indexName))
            );
        }
    }

    public void testAddIndexNodes() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();

        String indexName = "myindex";
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);

        GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(randomBoolean(), 10))
            .get();
        HealthIndicatorResult hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.GREEN));

        // Stop a node
        internalCluster().stopNode(indexNode);

        hir = waitForStatusAndGet(HealthStatus.RED);
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Cannot add data to 1 index [myindex]. Searches might return incomplete results.")
        );
        assertThat(hir.symptom(), containsString("This cluster has 1 unavailable primary shard."));
        assertThat(hir.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = hir.diagnosisList().get(0);
        assertThat(diagnosis.definition().id(), equalTo("debug_node:role:index"));
        assertThat(
            diagnosis.affectedResources()
                .stream()
                .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                .flatMap(r -> r.getValues().stream())
                .toList(),
            equalTo(List.of(indexName))
        );
    }

    /**
     * Wait for the health report API to report a certain high-level color and return the response
     */
    private HealthIndicatorResult waitForStatusAndGet(HealthStatus color) throws Exception {
        String indicator = "shards_availability";
        HealthIndicatorResult[] result = new HealthIndicatorResult[1];
        assertBusy(() -> {
            logger.info("--> waiting for cluster to be {}...", color);
            GetHealthAction.Request request = new GetHealthAction.Request(indicator, true, 10);
            var health = client().execute(GetHealthAction.INSTANCE, request).get();
            assertThat(health.findIndicator(indicator).status(), equalTo(color));
            result[0] = health.findIndicator(indicator);
        });
        return result[0];
    }
}
