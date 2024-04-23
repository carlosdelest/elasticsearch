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

import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexSizeMetricsCollectorTests extends ESTestCase {
    protected final ClusterSettings clusterSettings = new ClusterSettings(
        Settings.builder().put(SEARCH_POWER_MIN_SETTING.getKey(), 100).put(SEARCH_POWER_MAX_SETTING.getKey(), 200).build(),
        Set.of(SEARCH_POWER_MIN_SETTING, SEARCH_POWER_MAX_SETTING, SEARCH_POWER_SETTING)
    );

    public void testGetMetrics() throws IOException {
        String indexName = "myIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, 1, 1, 1, 1)) {
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );
            Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

            assertThat(metrics, hasSize(1));
            int shardIdInt = 0;
            var metric = (MetricsCollector.MetricValue) metrics.toArray()[0];
            assertThat(metric.measurementType(), equalTo(MetricsCollector.MeasurementType.SAMPLED));
            assertThat(metric.id(), equalTo("shard-size:" + indexName + ":" + shardIdInt));
            assertThat(metric.type(), equalTo("es_indexed_data"));
            assertThat(metric.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shardIdInt)));
            assertThat(metric.value(), greaterThan(1_000L));
        }
    }

    public void testMultipleShards() throws IOException {
        String indexName = "myMultiShardIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, 1, 10, 2, 3)) {
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );
            Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

            assertThat(metrics, hasSize(10));
            int shard = 0;
            for (MetricsCollector.MetricValue metric : metrics) {
                assertThat(metric.measurementType(), equalTo(MetricsCollector.MeasurementType.SAMPLED));
                assertThat(metric.id(), equalTo("shard-size:" + indexName + ":" + shard));
                assertThat(metric.type(), equalTo("es_indexed_data"));
                assertThat(metric.metadata(), equalTo(Map.of("index", indexName, "shard", "" + shard)));
                assertThat(metric.value(), greaterThan(1_000L));
                shard++;
            }
        }
    }

    public void testFailedShards() throws IOException {
        String indexName = "myMultiShardIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, 1, 10, 3, 9)) {
            int failedIndex = 7;
            var failed = testIndex.directories.get(failedIndex);
            for (var file : failed.listAll()) {
                failed.deleteFile(file);
            }
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );
            Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

            assertThat(metrics, hasSize(10));
            var hasPartial = hasEntry("partial", "" + true);
            int shard = 0;
            for (MetricsCollector.MetricValue metric : metrics) {
                assertThat(metric.measurementType(), equalTo(MetricsCollector.MeasurementType.SAMPLED));
                assertThat(metric.id(), equalTo("shard-size:" + indexName + ":" + shard));
                assertThat(metric.type(), equalTo("es_indexed_data"));
                assertThat(metric.metadata(), hasEntry("index", indexName));
                assertThat(metric.metadata(), hasEntry("shard", "" + shard));
                if (shard == failedIndex) {
                    assertThat(metric.value(), is(0L));
                    assertThat(metric.metadata(), hasPartial);
                } else {
                    assertThat(metric.value(), greaterThan(1_000L));
                    assertThat(metric.metadata(), Matchers.not(hasPartial));
                }
                shard++;
            }
        }
    }

    public void testNullEngine() {
        var indicesService = mock(IndicesService.class);
        var indexService = mock(IndexService.class);
        when(indicesService.iterator()).thenReturn(List.of(indexService).iterator());

        var index = mock(Index.class);
        when(indexService.index()).thenReturn(index);
        var indexName = "myIndex";
        when(index.getName()).thenReturn(indexName);

        var shard = mock(IndexShard.class);
        when(indexService.iterator()).thenReturn(List.of(shard).iterator());
        var shardId = mock(ShardId.class);
        when(shard.shardId()).thenReturn(shardId);
        int shardIdInt = 100;
        when(shardId.id()).thenReturn(shardIdInt);

        when(shard.getEngineOrNull()).thenReturn(null);

        IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
            indicesService,
            clusterSettings,
            Settings.EMPTY
        );
        Collection<MetricsCollector.MetricValue> metrics = indexSizeMetricsCollector.getMetrics();

        assertThat(metrics, hasSize(0));
    }

    public void testConcurrencyOneShardNoWait() throws InterruptedException, IOException {
        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();

        final int threadsCount = randomIntBetween(4, 10);
        final int opsPerThread = randomIntBetween(100, 2000);

        final int totalOps = opsPerThread * threadsCount;

        String indexName = "myIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, 1, 1, 2, 3)) {
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );

            final long sequentialReadMetricSum = indexSizeMetricsCollector.getMetrics()
                .stream()
                .mapToLong(MetricsCollector.MetricValue::value)
                .sum();

            ConcurrencyTestUtils.runConcurrent(
                threadsCount,
                opsPerThread,
                () -> 0,
                () -> results.addAll(indexSizeMetricsCollector.getMetrics()),
                logger::info
            );

            long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();

            assertThat(results, hasSize(totalOps));
            assertThat(valueSum, equalTo(opsPerThread * threadsCount * sequentialReadMetricSum));
        }
    }

    public void testConcurrencyOneShardRandomWait() throws InterruptedException, IOException {
        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();

        final int threadsCount = randomIntBetween(4, 10);
        final int opsPerThread = randomIntBetween(50, 200);

        final int totalOps = opsPerThread * threadsCount;

        String indexName = "myIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, 1, 1, 2, 3)) {
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );

            final long sequentialReadMetricSum = indexSizeMetricsCollector.getMetrics()
                .stream()
                .mapToLong(MetricsCollector.MetricValue::value)
                .sum();

            ConcurrencyTestUtils.runConcurrent(
                threadsCount,
                opsPerThread,
                () -> randomIntBetween(0, 50),
                () -> results.addAll(indexSizeMetricsCollector.getMetrics()),
                logger::info
            );

            long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();

            assertThat(results, hasSize(totalOps));
            assertThat(valueSum, equalTo(opsPerThread * threadsCount * sequentialReadMetricSum));
        }
    }

    public void testConcurrencyMultipleShardsRandomWait() throws InterruptedException, IOException {
        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();

        final int threadsCount = randomIntBetween(4, 10);
        final int opsPerThread = randomIntBetween(50, 200);
        final int numberOfShards = 10;

        final int totalOps = opsPerThread * threadsCount * numberOfShards;

        String indexName = "myIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, 1, numberOfShards, 2, 3)) {
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );

            final long sequentialReadMetricSum = indexSizeMetricsCollector.getMetrics()
                .stream()
                .mapToLong(MetricsCollector.MetricValue::value)
                .sum();

            ConcurrencyTestUtils.runConcurrent(
                threadsCount,
                opsPerThread,
                () -> randomIntBetween(0, 50),
                () -> results.addAll(indexSizeMetricsCollector.getMetrics()),
                logger::info
            );

            long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();

            assertThat(results, hasSize(totalOps));
            assertThat(valueSum, equalTo(opsPerThread * threadsCount * sequentialReadMetricSum));
        }
    }

    public void testConcurrencyMultipleIndicesRandomWait() throws InterruptedException, IOException {
        final var results = new ConcurrentLinkedQueue<MetricsCollector.MetricValue>();

        final int threadsCount = randomIntBetween(4, 10);
        final int opsPerThread = randomIntBetween(20, 100);
        final int numberOfIndexes = 5;
        final int numberOfShards = 5;

        final int totalOps = opsPerThread * threadsCount * numberOfIndexes * numberOfShards;

        String indexName = "myIndex";
        try (TestIndex testIndex = setUpIndicesService(indexName, numberOfIndexes, numberOfShards, 2, 3)) {
            IndexSizeMetricsCollector indexSizeMetricsCollector = new IndexSizeMetricsCollector(
                testIndex.indicesService,
                clusterSettings,
                Settings.EMPTY
            );

            final long sequentialReadMetricSum = indexSizeMetricsCollector.getMetrics()
                .stream()
                .mapToLong(MetricsCollector.MetricValue::value)
                .sum();

            ConcurrencyTestUtils.runConcurrent(
                threadsCount,
                opsPerThread,
                () -> randomIntBetween(0, 50),
                () -> results.addAll(indexSizeMetricsCollector.getMetrics()),
                logger::info
            );

            long valueSum = results.stream().mapToLong(MetricsCollector.MetricValue::value).sum();

            assertThat(results, hasSize(totalOps));
            assertThat(valueSum, equalTo(opsPerThread * threadsCount * sequentialReadMetricSum));
        }
    }

    static TestIndex setUpIndicesService(String indexName, int numIndices, int numShards, int commitsPerShard, int docsPerCommit)
        throws IOException {
        var indicesService = mock(IndicesService.class);
        TestIndex testIndex = new TestIndex(indicesService, new ArrayList<>(numShards * commitsPerShard), commitsPerShard);

        var indexService = mock(IndexService.class);
        when(indicesService.iterator()).then(a -> Collections.nCopies(numIndices, indexService).iterator());

        var index = mock(Index.class);
        when(indexService.index()).thenReturn(index);
        when(index.getName()).thenReturn(indexName);

        List<IndexShard> mockShards = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            var shard = mock(IndexShard.class);
            var shardId = mock(ShardId.class);
            when(shard.shardId()).thenReturn(shardId);
            when(shardId.id()).thenReturn(i);
            var engine = mock(Engine.class);
            when(shard.getEngineOrNull()).thenReturn(engine);
            var dir = generateSegmentInfosDir(i, docsPerCommit, commitsPerShard);
            testIndex.directories.add(dir);
            when(engine.getLastCommittedSegmentInfos()).thenReturn(SegmentInfos.readLatestCommit(dir));
            mockShards.add(shard);
        }

        when(indexService.iterator()).thenAnswer(a -> Collections.unmodifiableList(mockShards).iterator());

        return testIndex;
    }

    record TestIndex(IndicesService indicesService, List<Directory> directories, int commitsPerShard) implements AutoCloseable {
        public void close() {
            try {
                IOUtils.close(directories);
            } catch (IOException err) {
                // ignore
            }
        }
    }

    private static Directory generateSegmentInfosDir(int id, int docsPerCommit, int commits) throws IOException {
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, iwc);
        for (int i = 0; i < commits; i++) {
            for (int j = 0; j < docsPerCommit; j++) {
                writer.addDocument(
                    Arrays.asList(
                        new StringField("id", Integer.toString(id), Field.Store.YES),
                        new SortedNumericDocValuesField("num", id + 1_000)
                    )
                );
                id++;
            }
            writer.commit();
        }
        writer.close();
        return dir;
    }

    private Directory generateSegmentInfosDir(int id) throws IOException {
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, iwc);
        writer.addDocument(
            Arrays.asList(new StringField("id", Integer.toString(id), Field.Store.YES), new SortedNumericDocValuesField("num", id + 1_000))
        );
        writer.commit();
        writer.close();
        return dir;
    }
}
