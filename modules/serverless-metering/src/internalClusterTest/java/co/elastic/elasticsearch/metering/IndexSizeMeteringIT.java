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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

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

    public void testSizeMetricsAreRecorded() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        String indexName = "idx1";
        createIndex(indexName);
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        String idPRefix = "shard-size:" + indexName + ":0";
        var usageRecords = new ArrayList<UsageRecord>();
        assertBusy(() -> {
            pollReceivedRecords(usageRecords);
            var lastUsageRecord = usageRecords.stream()
                .filter(m -> m.id().startsWith(idPRefix))
                .max(Comparator.comparing(UsageRecord::usageTimestamp));

            var metric = lastUsageRecord.orElseThrow(() -> new AssertionError("No IX usage records for " + indexName));

            assertThat(metric.id(), startsWith(idPRefix));
            assertThat(metric.usage().type(), equalTo("es_indexed_data"));
            assertThat(metric.usage().quantity(), greaterThan(0L));
            assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName, "shard", "0")));

        }, 20, TimeUnit.SECONDS);
    }
}
