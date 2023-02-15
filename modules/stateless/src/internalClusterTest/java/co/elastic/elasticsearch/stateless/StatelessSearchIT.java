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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesRequestCacheUtils;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class StatelessSearchIT extends AbstractStatelessIntegTestCase {

    /**
     * A testing stateless plugin that extends the {@link Engine.IndexCommitListener} to count created number of commits.
     */
    public static class TestStateless extends Stateless {

        private final AtomicInteger createdCommits = new AtomicInteger(0);

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected Engine.IndexCommitListener createIndexCommitListener() {
            Engine.IndexCommitListener superListener = super.createIndexCommitListener();
            return new Engine.IndexCommitListener() {

                @Override
                public void onNewCommit(
                    ShardId shardId,
                    Store store,
                    long primaryTerm,
                    Engine.IndexCommitRef indexCommitRef,
                    Set<String> additionalFiles
                ) {
                    createdCommits.incrementAndGet();
                    superListener.onNewCommit(shardId, store, primaryTerm, indexCommitRef, additionalFiles);
                }

                @Override
                public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                    superListener.onIndexCommitDelete(shardId, deletedCommit);
                }
            };
        }

        private int getCreatedCommits() {
            return createdCommits.get();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlobCachePlugin.class, MockTransportService.TestPlugin.class, TestStateless.class);
    }

    private static int getNumberOfCreatedCommits() {
        int numberOfCreatedCommits = 0;
        for (String node : internalCluster().getNodeNames()) {
            var plugin = internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestStateless.class).get(0);
            numberOfCreatedCommits += plugin.getCreatedCommits();
        }
        return numberOfCreatedCommits;
    }

    private final int numShards = randomIntBetween(1, 3);
    private final int numReplicas = randomIntBetween(1, 2);

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    public void testSearchShardsStarted() {
        startIndexNodes(numShards);
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedAfterIndexShards() {
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedWithDocs() {
        startIndexNodes(numShards);
        startSearchNodes(numShards * numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }
        ensureGreen(indexName);
    }

    public void testSearchShardsStartedAfterIndexShardsWithDocs() {
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }

        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(1, 100));
        if (randomBoolean()) {
            var flushResponse = flush(indexName);
            assertEquals(RestStatus.OK, flushResponse.getStatus());
        }
        ensureGreen(indexName);
    }

    public void testStatelessShardsSupportGet() {
        // Currently this test depends on routing requests to indexing shards. However, eventually these
        // requests will route to search shards and fall back to indexing shards in certain circumstances.
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        startSearchNodes(numShards * numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        ensureGreen(indexName);

        var bulkRequest = client().prepareBulk();
        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        bulkRequest.setRefreshPolicy(IMMEDIATE); // to ensure search shards are up-to-date
        BulkResponse response = bulkRequest.get();
        assertNoFailures(response);
        String id = response.getItems()[0].getResponse().getId();
        GetResponse getResponse = client().prepareGet().setIndex(indexName).setId(id).get();
        assertTrue(getResponse.isExists());

        String id2 = response.getItems()[1].getResponse().getId();
        MultiGetResponse multiGetResponse = client().prepareMultiGet().addIds(indexName, id, id2).get();
        assertTrue(multiGetResponse.getResponses()[0].getResponse().isExists());
        assertTrue(multiGetResponse.getResponses()[1].getResponse().isExists());
    }

    public void testSearchShardsNotifiedOnNewCommits() throws Exception {
        startIndexNodes(numShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        startSearchNodes(numReplicas);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        ensureGreen(indexName);

        final AtomicInteger searchNotifications = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addRequestHandlingBehavior(
                TransportNewCommitNotificationAction.NAME + "[u]",
                (handler, request, channel, task) -> {
                    searchNotifications.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }
            );
        }

        final int beginningNumberOfCreatedCommits = getNumberOfCreatedCommits();

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush().setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh().get();
                case 2 -> client().admin().indices().prepareForceMerge().get();
            }
        }

        assertBusy(() -> {
            assertThat(
                "Search shard notifications should be equal to the number of created commits multiplied by the number of replicas.",
                searchNotifications.get(),
                equalTo((getNumberOfCreatedCommits() - beginningNumberOfCreatedCommits) * numReplicas)
            );
        });
    }

    public void testRefresh() throws Exception {
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .build()
        );
        ensureGreen(indexName);
        int docsToIndex = randomIntBetween(1, 100);
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < docsToIndex; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        boolean bulkRefreshes = true;
        if (bulkRefreshes) {
            bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        if (bulkRefreshes == false) {
            assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        } else {
            // Currently anything other than NONE would result in the forced_refresh set to true.
            // TODO: refine this once https://elasticco.atlassian.net/browse/ES-5312 is done.
            for (BulkItemResponse response : bulkResponse.getItems()) {
                if (response.getResponse() != null) {
                    assertTrue(response.getResponse().forcedRefresh());
                }
            }
        }
        var searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertEquals(docsToIndex, searchResponse.getHits().getTotalHits().value);
    }

    public void testScrollingSearchNotInterruptedByNewCommit() throws Exception {
        // Use one replica to ensure both searches hit the same shard
        final int numReplicas = 1;
        startIndexNodes(numShards);
        startSearchNodes(numReplicas);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false);
        if (randomBoolean()) {
            indexSettings.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1);
        }
        createIndex(indexName, indexSettings.build());
        ensureGreen(indexName);

        int bulk1DocsToIndex = randomIntBetween(10, 100);
        Set<String> bulk1DocIds = indexDocsWithRefreshAndGetIds(indexName, bulk1DocsToIndex);
        Set<String> lastBulkIds = bulk1DocIds;
        long docsIndexed = bulk1DocsToIndex;
        int scrollSize = randomIntBetween(10, 100);
        long docsDeleted = 0;
        int scrolls = (int) Math.ceil((float) bulk1DocsToIndex / scrollSize);
        // The scrolling search should only see docs from the first bulk
        SearchResponse scrollSearchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(scrollSize)
            .setScroll(TimeValue.timeValueMinutes(2))
            .get();
        assertNoFailures(scrollSearchResponse);
        assertThat(scrollSearchResponse.getHits().getTotalHits().value, equalTo((long) bulk1DocsToIndex));
        Set<String> scrollSearchDocsSeen = Arrays.stream(scrollSearchResponse.getHits().getHits())
            .map(SearchHit::getId)
            .collect(Collectors.toSet());
        try {
            for (int i = 1; i < scrolls; i++) {
                if (randomBoolean()) {
                    // delete at least one doc
                    int docsToDelete = randomIntBetween(1, lastBulkIds.size());
                    var deletedDocIds = randomSubsetOf(docsToDelete, lastBulkIds);
                    deleteDocsById(indexName, deletedDocIds);
                    docsDeleted += deletedDocIds.size();
                }
                var docsToIndex = randomIntBetween(10, 100);
                lastBulkIds = indexDocsWithRefreshAndGetIds(indexName, docsToIndex);
                docsIndexed += docsToIndex;
                // make sure new docs are visible to new searches
                var searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
                assertNoFailures(searchResponse);
                assertEquals(docsIndexed - docsDeleted, searchResponse.getHits().getTotalHits().value);
                // fetch next scroll
                scrollSearchResponse = client().prepareSearchScroll(scrollSearchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .get();
                assertNoFailures(scrollSearchResponse);
                assertThat(scrollSearchResponse.getHits().getTotalHits().value, equalTo((long) bulk1DocsToIndex));
                scrollSearchDocsSeen.addAll(
                    Arrays.stream(scrollSearchResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet())
                );
            }
            assertThat(scrollSearchDocsSeen, equalTo(bulk1DocIds));
        } finally {
            clearScroll(scrollSearchResponse.getScrollId());
        }
    }

    public void testSearchNotInterruptedByNewCommit() throws Exception {
        // Use one replica to ensure both searches hit the same shard
        final int numReplicas = 1;
        // Use at least two shards to ensure there will always be a FETCH phase
        final int numShards = randomIntBetween(2, 3);
        startIndexNodes(numShards);
        String coordinatingSearchNode = startSearchNode();
        startSearchNodes(numReplicas);
        // create index on all nodes except one search node
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
            .put("index.routing.allocation.exclude._name", coordinatingSearchNode);
        createIndex(indexName, indexSettings.build());
        ensureGreen(indexName);
        int bulk1DocsToIndex = randomIntBetween(100, 200);
        indexDocsAndRefresh(indexName, bulk1DocsToIndex);
        // Index more docs in between the QUERY and the FETCH phase of the search
        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            coordinatingSearchNode
        );
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch secondBulkIndexed = new CountDownLatch(1);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(SearchTransportService.FETCH_ID_ACTION_NAME)) {
                try {
                    fetchStarted.countDown();
                    secondBulkIndexed.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        CountDownLatch searchFinished = new CountDownLatch(1);
        client(coordinatingSearchNode).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).execute(new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) bulk1DocsToIndex));
                searchFinished.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });
        fetchStarted.await();
        int bulk2DocsToIndex = randomIntBetween(10, 100);
        indexDocsAndRefresh(indexName, bulk2DocsToIndex);
        // Verify that new docs are visible to new searches
        var search2Response = client(coordinatingSearchNode).prepareSearch(indexName)
            .setSize(0)  // Avoid a FETCH phase
            .setQuery(QueryBuilders.matchAllQuery())
            .get();
        assertNoFailures(search2Response);
        assertEquals(bulk1DocsToIndex + bulk2DocsToIndex, search2Response.getHits().getTotalHits().value);
        secondBulkIndexed.countDown();
        searchFinished.await();
    }

    public void testRequestCache() {
        startMasterOnlyNode();
        int numberOfShards = 1;
        startIndexNodes(numberOfShards);
        startSearchNodes(numberOfShards);
        String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfShards)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(indexName);

        List<Integer> data = randomList(4, 64, ESTestCase::randomInt);
        for (int i = 0; i < data.size(); i++) {
            indexDocWithRange(indexName, String.valueOf(i + 1), data.get(i));
        }
        refresh(indexName);

        // Use a fixed client in order to avoid randomizing timeouts which leads to different cache entries
        var client = client();
        assertRequestCacheStats(client, indexName, equalTo(0L), 0, 0);

        int min = Collections.min(data);
        int max = Collections.max(data);
        var cacheMiss = countDocsInRange(client, indexName, min, max);
        assertThat(cacheMiss.getHits().getTotalHits().value, equalTo((long) data.size()));
        assertRequestCacheStats(client, indexName, greaterThan(0L), 0, 1);

        int nbSearchesWithCacheHits = randomIntBetween(1, 10);
        for (int i = 0; i < nbSearchesWithCacheHits; i++) {
            var cacheHit = countDocsInRange(client, indexName, min, max);
            assertThat(cacheHit.getHits().getTotalHits().value, equalTo((long) data.size()));
            assertRequestCacheStats(client, indexName, greaterThan(0L), i + 1, 1);
        }

        List<Integer> moreData = randomList(4, 64, () -> randomIntBetween(min, max));
        for (int i = 0; i < moreData.size(); i++) {
            indexDocWithRange(indexName, String.valueOf(data.size() + i + 1), moreData.get(i));
        }
        // refresh forces a reopening of the reader on the search shard. Because the reader is part of the request
        // cache key further count requests will account for cache misses
        refresh(indexName);

        var cacheMissDueRefresh = countDocsInRange(client, indexName, min, max);
        assertThat(cacheMissDueRefresh.getHits().getTotalHits().value, equalTo((long) (data.size() + moreData.size())));
        assertRequestCacheStats(client, indexName, greaterThan(0L), nbSearchesWithCacheHits, 2);

        // Verify that the request cache evicts the closed index
        client().admin().indices().prepareClose(indexName).get();
        ensureGreen(indexName);
        for (var indicesService : internalCluster().getInstances(IndicesService.class)) {
            var indicesRequestCache = IndicesRequestCacheUtils.getRequestCache(indicesService);
            IndicesRequestCacheUtils.cleanCache(indicesRequestCache);
            assertThat(Iterables.size(IndicesRequestCacheUtils.cachedKeys(indicesRequestCache)), equalTo(0L));
        }
    }

    public void testIndexSort() {
        startMasterOnlyNode();
        final int numberOfShards = 1;
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                    .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "rank")
            ).setMapping("rank", "type=integer").get()
        );
        ensureGreen(indexName);

        index(indexName, "1", Map.of("rank", 4));
        index(indexName, "2", Map.of("rank", 1));
        index(indexName, "3", Map.of("rank", 3));
        index(indexName, "4", Map.of("rank", 2));

        refresh(indexName);

        index(indexName, "5", Map.of("rank", 8));
        index(indexName, "6", Map.of("rank", 6));
        index(indexName, "7", Map.of("rank", 5));
        index(indexName, "8", Map.of("rank", 7));

        refresh(indexName);

        startSearchNodes(1);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);

        SearchResponse searchResponse = client().prepareSearch(indexName)
            .setSource(new SearchSourceBuilder().sort("rank"))
            .setSize(1)
            .get();
        assertHitCount(searchResponse, 8L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));

        searchResponse = client().prepareSearch(indexName)
            .setSource(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("rank").from(0)).sort("rank"))
            .setTrackTotalHits(false)
            .setSize(1)
            .get();
        assertThat(searchResponse.getHits().getTotalHits(), nullValue());
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));

        assertNoFailures(client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        refresh(indexName);

        searchResponse = client().prepareSearch(indexName).setSource(new SearchSourceBuilder().sort("_doc")).get();
        assertHitCount(searchResponse, 8L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(8));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(3).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(4).getId(), equalTo("7"));
        assertThat(searchResponse.getHits().getAt(5).getId(), equalTo("6"));
        assertThat(searchResponse.getHits().getAt(6).getId(), equalTo("8"));
        assertThat(searchResponse.getHits().getAt(7).getId(), equalTo("5"));

        searchResponse = client().prepareSearch(indexName)
            .setSource(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("rank").from(0)).sort("rank"))
            .setTrackTotalHits(false)
            .setSize(3)
            .get();

        assertThat(searchResponse.getHits().getTotalHits(), nullValue());
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        var exception = expectThrows(
            ActionRequestValidationException.class,
            () -> client().prepareSearch(indexName)
                .setSource(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("rank").from(0)).sort("rank"))
                .setTrackTotalHits(false)
                .setScroll(TimeValue.timeValueMinutes(1))
                .setSize(3)
                .get()
        );
        assertThat(exception.getMessage(), containsString("disabling [track_total_hits] is not allowed in a scroll context"));
    }

    private static SearchResponse countDocsInRange(Client client, String index, int min, int max) {
        SearchResponse response = client.prepareSearch(index)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setSize(0) // index request cache only supports count requests
            .setQuery(QueryBuilders.rangeQuery("f").gte(min).lte(max))
            .get();
        assertSearchResponse(response);
        return response;
    }

    private static void assertRequestCacheStats(
        Client client,
        String index,
        Matcher<Long> memorySize,
        long expectedHits,
        long expectedMisses
    ) {
        var requestCache = client.admin().indices().prepareStats(index).setRequestCache(true).get().getTotal().getRequestCache();
        assertThat(requestCache.getMemorySize().getBytes(), memorySize);
        assertThat(requestCache.getHitCount(), equalTo(expectedHits));
        assertThat(requestCache.getMissCount(), equalTo(expectedMisses));
    }

    private static void indexDocWithRange(String index, String id, int value) {
        assertThat(client().prepareIndex(index).setId(id).setSource("f", value).get().status(), equalTo(RestStatus.CREATED));
    }

    private Set<String> indexDocsWithRefreshAndGetIds(String indexName, int numDocs) throws Exception {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        boolean bulkRefreshes = randomBoolean();
        if (bulkRefreshes) {
            bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        }
        assertNoFailures(bulkRequest.get());
        if (bulkRefreshes == false) {
            assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        }
        return bulkRequest.request().requests().stream().map(DocWriteRequest::id).collect(Collectors.toSet());
    }

    private void deleteDocsById(String indexName, Collection<String> docIds) {
        var bulkRequest = client().prepareBulk();
        for (String id : docIds) {
            bulkRequest.add(new DeleteRequest(indexName, id));
        }
        assertNoFailures(bulkRequest.get());
    }
}
