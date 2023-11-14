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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ServerlessShardsHealthIT extends AbstractStatelessIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new HashSet<>(super.nodePlugins());
        plugins.add(ServerlessShardsHealthPlugin.class);
        return plugins;
    }

    public void testReplicaShardsAvailabilityOnServerless() throws Exception {
        startMasterAndIndexNode();

        String indexName = "myindex";
        createIndex(indexName, 2, 0);
        ensureGreen(indexName);

        GetHealthAction.Response health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(randomBoolean(), 10))
            .get();
        HealthIndicatorResult hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.GREEN));

        // Increase the number of replicas to 2, so that the index goes red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.number_of_replicas", 2))
            .get();

        health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();

        hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.RED));
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Not all data is searchable. No searchable copies of the data exist on 1 index [myindex].")
        );
        assertThat(hir.symptom(), containsString("This cluster has 4 unavailable replica shards."));
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
        waitFor(HealthStatus.YELLOW);
        health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
        hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.YELLOW));
        // We should inherit the existing impacts from the Stateful code version
        assertThat(
            Strings.collectionToCommaDelimitedString(hir.impacts().stream().map(HealthIndicatorImpact::impactDescription).toList()),
            containsString("Searches might be slower than usual. Fewer redundant copies of the data exist on 1 index [myindex].")
        );
        assertThat(hir.symptom(), containsString("This cluster has 2 unavailable replica shards."));
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
        waitFor(HealthStatus.GREEN);
        health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
        hir = health.findIndicator("shards_availability");
        assertThat(hir.status(), equalTo(HealthStatus.GREEN));
    }

    /**
     * Wait for the health report API to report a certain high-level color
     */
    private void waitFor(HealthStatus color) throws Exception {
        assertBusy(() -> {
            logger.info("--> waiting for cluster to be {}...", color);
            var health = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 10)).get();
            assertThat(health.getStatus(), equalTo(color));
        });
    }
}
