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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;
import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;

public class IndexSizeMeteringIT extends AbstractMeteringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), ProjectType.ELASTICSEARCH_GENERAL_PURPOSE)
            .build();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    protected int numberOfShards() {
        return 1;
    }

    public void testNodeCanStartWithMeteringEnabled() {
        startMasterAndIndexNode();
        startSearchNode();
        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(MeteringPlugin.class))
            .toList();
        assertThat(plugins, not(empty()));
    }

    private Predicate<UsageRecord> isShardSizeMetric(String indexName, int shardId) {
        return m -> m.id().startsWith("shard-size:" + indexName + ":" + shardId);
    }

    private Predicate<UsageRecord> isIndexSizeMetric(String indexName) {
        return m -> m.id().startsWith("index-size:" + indexName);
    }

    public void testIndexSizeMetricsAreRecorded() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        createIndex("idx1");
        client().index(new IndexRequest("idx1").source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest("idx1").force(true)).actionGet();

        var usageRecords = new ArrayList<UsageRecord>();
        assertBusy(() -> {
            pollReceivedRecords(usageRecords, isIndexSizeMetric("idx1"));
            var lastUsageRecord = usageRecords.stream().max(Comparator.comparing(UsageRecord::usageTimestamp));

            var metric = lastUsageRecord.orElseThrow(() -> new AssertionError("No IX usage records for " + "idx1"));

            assertThat(metric.usage().type(), equalTo("es_indexed_data"));
            assertThat(metric.usage().quantity(), greaterThan(0L));
            assertThat(
                metric.source().metadata(),
                allOf(hasEntry("index", "idx1"), hasEntry("system_index", "false"), hasEntry("hidden_index", "false"))
            );

        }, 20, TimeUnit.SECONDS);
    }

    public void testShardSizeMetricsAreRecorded() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        createIndex("idx1");
        client().index(new IndexRequest("idx1").source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest("idx1").force(true)).actionGet();

        var usageRecords = new ArrayList<UsageRecord>();
        assertBusy(() -> {
            pollReceivedRecords(usageRecords, isShardSizeMetric("idx1", 0));
            var lastUsageRecord = usageRecords.stream().max(Comparator.comparing(UsageRecord::usageTimestamp));

            var metric = lastUsageRecord.orElseThrow(() -> new AssertionError("No IX usage records for " + "idx1"));

            assertThat(metric.usage().type(), equalTo("es_indexed_data"));
            assertThat(metric.usage().quantity(), greaterThan(0L));
            assertThat(
                metric.source().metadata(),
                allOf(
                    hasEntry("index", "idx1"),
                    hasEntry("system_index", "false"),
                    hasEntry("hidden_index", "false"),
                    hasEntry("shard", "0")
                )
            );

        }, 20, TimeUnit.SECONDS);
    }

}
