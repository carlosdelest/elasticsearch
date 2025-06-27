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

package co.elastic.elasticsearch.downsample.serverless;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.downsample.Downsample;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class StatelessDownsampleIT extends AbstractStatelessIntegTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final TimeValue WAIT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);
    private static final String DIMENSION = "dimension_1";
    private static final String COUNTER = "counter";

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(Downsample.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(AggregateMetricMapperPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0);
    }

    public void testDownsamplingHollowIndex() throws Exception {
        final var indexingNodeA = startMasterAndIndexNode();
        startSearchNode();
        final String indexName = "my-hollow-index";
        Instant now = Instant.now();
        long startTime = now.minusSeconds(60 * 60).toEpochMilli();
        long endTime = now.plusSeconds(60 * 29).toEpochMilli();

        createTimeSeriesIndex(indexName, startTime, endTime);
        ensureGreen(indexName);

        indexTimeSeriesDocs(indexName, 100, startTime, endTime);
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build())
        );

        final var hollowShardsServiceA = internalCluster().getInstance(HollowShardsService.class, indexingNodeA);
        final var indexShard = findIndexShard(indexName);
        assertBusy(() -> assertThat(hollowShardsServiceA.isHollowableIndexShard(indexShard), equalTo(true)));

        // Start second indexing node so we can trigger a relocation that will make the shard hollow
        final var indexingNodeB = startIndexNode();
        hollowShards(indexName, 1, indexingNodeA, indexingNodeB);

        // Start the downsampling
        String interval = "5m";
        String targetIndex = "downsample-" + interval + "-" + indexName;
        DownsampleConfig downsampleConfig = new DownsampleConfig(new DateHistogramInterval(interval));
        assertAcked(
            client().execute(
                DownsampleAction.INSTANCE,
                new DownsampleAction.Request(TEST_REQUEST_TIMEOUT, indexName, targetIndex, WAIT_TIMEOUT, downsampleConfig)
            )
        );
        // Wait for downsampling to complete
        SubscribableListener<Void> listener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            final var indexMetadata = clusterState.metadata().getProject().index(targetIndex);
            if (indexMetadata == null) {
                return false;
            }
            var downsampleStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
            return downsampleStatus == IndexMetadata.DownsampleTaskStatus.SUCCESS;
        });
        safeAwait(listener);

        refresh(targetIndex);
        // Ensure we have at least 1 downsampled document
        assertResponse(
            prepareSearch(targetIndex),
            response -> assertThat(response.getHits().getTotalHits().value(), greaterThanOrEqualTo(1L))
        );
    }

    private void createTimeSeriesIndex(String indexName, long startTime, long endTime) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), DIMENSION)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), DATE_FORMATTER.formatMillis(startTime))
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), DATE_FORMATTER.formatMillis(endTime))
            .build();
        String mapping = String.format(Locale.ROOT, """
            {
              "properties": {
                "@timestamp": {
                  "type": "date"
                },
                "%s": {
                  "type": "keyword",
                  "time_series_dimension": true
                },
                "%s": {
                  "type": "double",
                  "time_series_metric": "counter"
                }
              }
            }""", DIMENSION, COUNTER);
        assertAcked(prepareCreate(indexName).setSettings(settings).setMapping(mapping).get());
    }

    private void indexTimeSeriesDocs(String indexName, int docCount, long startTime, long endtime) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(indexName).opType(DocWriteRequest.OpType.CREATE);
            try (var builder = XContentFactory.jsonBuilder()) {
                long timestamp = randomLongBetween(startTime, endtime);
                indexRequest.source(
                    builder.startObject()
                        .field("@timestamp", DATE_FORMATTER.formatMillis(timestamp))
                        .field(DIMENSION, randomFrom("host1", "host2", "host3"))
                        .field(COUNTER, i)
                        .endObject()
                );
                bulkRequestBuilder.add(indexRequest);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        assertThat(bulkItemResponses.hasFailures(), equalTo(false));
    }
}
