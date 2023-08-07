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

import co.elastic.elasticsearch.metering.reports.UsageRecord;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IndexSizeMeteringIT extends AbstractMeteringIntegTestCase {

    public void testNodeCanStartWithMeteringEnabled() {
        startNodes();

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(MeteringPlugin.class).stream())
            .toList();
        assertThat(plugins, not(empty()));
    }

    public void testSizeMetricsAreRecorded() throws InterruptedException {
        startNodes();

        String indexName = "idx1";
        assertAcked(
            prepareCreate(
                indexName,
                1,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        client().index(new IndexRequest(indexName).source(XContentType.JSON, "value1", "foo", "value2", "bar")).actionGet();
        admin().indices().flush(new FlushRequest(indexName).force(true)).actionGet();

        waitUntil(() -> receivedMetrics().isEmpty() == false);
        assertThat(receivedMetrics(), not(empty()));
        UsageRecord metric = getUsageRecords("shard-size");

        String idPRefix = "shard-size:" + indexName + ":0";
        assertThat(metric.id(), startsWith(idPRefix));
        assertThat(metric.usage().type(), equalTo("es_indexed_data"));
        assertThat(metric.usage().quantity(), greaterThan(0L));
        assertThat(metric.source().metadata(), equalTo(Map.of("index", indexName, "shard", 0)));
    }
}
