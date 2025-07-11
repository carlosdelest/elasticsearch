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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkResponse;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesRequest;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.stateless.Stateless.CLEAR_BLOB_CACHE_ACTION;
import static org.elasticsearch.blobcache.BlobCacheUtils.toPageAlignedSize;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class SearchCommitPrefetcherIT extends AbstractStatelessIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStatelessNoRecoveryPrewarming.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Ensure that we have total control about how and when VBCCs are uploaded to the blob store
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(12))
            // Ensure that there's enough room to cache the data
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // Ensure that we always prefetch data from indexing nodes when required
            .put(SearchCommitPrefetcher.PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING.getKey(), ByteSizeValue.ofGb(20));
    }

    public void testCommitPrefetchingDisabledDoesNotDownloadTheEntireCommit() {
        var skipPrefetchingBecauseSearchIsIdle = randomBoolean();
        var prefetchingEnabled = skipPrefetchingBecauseSearchIsIdle == false;
        var prefetchNonUploadedCommits = randomBoolean();
        var nodeSettings = Settings.builder()
            .put(SearchCommitPrefetcher.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), prefetchingEnabled)
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), prefetchNonUploadedCommits)
            .put(
                SearchCommitPrefetcher.PREFETCH_SEARCH_IDLE_TIME_SETTING.getKey(),
                skipPrefetchingBecauseSearchIsIdle ? TimeValue.ZERO : TimeValue.THIRTY_SECONDS
            )
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNode = startSearchNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var latestCommitGeneration = client().admin().indices().prepareStats(indexName).get().getAt(0).getCommitStats().getGeneration();
        var vBCCGen = latestCommitGeneration + 1;
        var shardId = new ShardId(resolveIndex(indexName), 0);
        var bccBlobName = BatchedCompoundCommit.blobNameFromGeneration(vBCCGen);

        var bytesReadFromBlobStore = meterBlobStoreReadsForBCC(searchNode, bccBlobName);
        var bytesReadFromIndexingNode = meterIndexingNodeReadsForBCC(indexNode, shardId, vBCCGen);

        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(0L)));

        var numberOfCommits = randomIntBetween(5, 10);
        for (int j = 0; j < numberOfCommits; j++) {
            // Index enough documents so the initial read happening during refresh doesn't include the complete Lucene files
            indexDocs(indexName, 10_000);
            refresh(indexName);
        }
        var currentVirtualBcc = internalCluster().getInstance(StatelessCommitService.class, indexNode).getCurrentVirtualBcc(shardId);
        var bccTotalSizeInBytes = currentVirtualBcc.getTotalSizeInBytes();

        var uploadBCC = prefetchNonUploadedCommits == false || randomBoolean();
        if (uploadBCC) {
            flush(indexName);
        }

        assertThat(bytesReadFromBlobStore.get(), is(equalTo(0L)));
        assertThat(bytesReadFromIndexingNode.get(), is(greaterThan(0L)));
        assertThat(bytesReadFromIndexingNode.get(), is(lessThan(bccTotalSizeInBytes)));

        var bytesReadFromIndexingNodeBeforeSearch = bytesReadFromIndexingNode.get();
        var bytesReadFromBlobStoreBeforeSearch = bytesReadFromBlobStore.get();

        var searchRequest = prepareSearch(indexName);
        if (randomBoolean()) {
            searchRequest.setQuery(new MatchAllQueryBuilder()).setSize(randomIntBetween(100, 10_000));
        } else {
            searchRequest.setQuery(new TermQueryBuilder("field", "non-existent"));
        }
        assertNoFailures(searchRequest);

        // Maybe there's a better search request that forces fetching more data?
        if (uploadBCC) {
            assertThat(bytesReadFromIndexingNode.get(), is(equalTo(bytesReadFromIndexingNodeBeforeSearch)));
            assertThat(bytesReadFromBlobStore.get(), is(greaterThanOrEqualTo(bytesReadFromBlobStoreBeforeSearch)));
        } else {
            assertThat(bytesReadFromIndexingNode.get(), is(greaterThanOrEqualTo(bytesReadFromIndexingNodeBeforeSearch)));
            assertThat(bytesReadFromBlobStore.get(), is(equalTo(bytesReadFromBlobStoreBeforeSearch)));
        }
    }

    public void testCommitPrefetching() throws Exception {
        var prefetchNonUploadedCommits = randomBoolean();
        var nodeSettings = Settings.builder()
            .put(SearchCommitPrefetcher.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), prefetchNonUploadedCommits)
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNode = startSearchNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var latestCommitGeneration = client().admin().indices().prepareStats(indexName).get().getAt(0).getCommitStats().getGeneration();
        var vBCCGen = latestCommitGeneration + 1;
        var shardId = new ShardId(resolveIndex(indexName), 0);
        var bccBlobName = BatchedCompoundCommit.blobNameFromGeneration(vBCCGen);

        var bytesReadFromBlobStore = meterBlobStoreReadsForBCC(searchNode, bccBlobName);
        var bytesReadFromIndexingNode = meterIndexingNodeReadsForBCC(indexNode, shardId, vBCCGen);

        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(0L)));

        var numberOfCommits = randomIntBetween(5, 10);
        for (int j = 0; j < numberOfCommits; j++) {
            // Index enough documents so the initial read happening during refresh doesn't include the complete Lucene files
            indexDocs(indexName, 10_000);
            refresh(indexName);
        }
        var currentVirtualBcc = internalCluster().getInstance(StatelessCommitService.class, indexNode).getCurrentVirtualBcc(shardId);
        var bccTotalPaddingInBytes = currentVirtualBcc.getTotalPaddingInBytes();
        var bccTotalSizeInBytes = currentVirtualBcc.getTotalSizeInBytes();

        flush(indexName);

        assertBusy(
            () -> assertThat(
                bccTotalSizeInBytes,
                // A BCC can contain multiple Lucene commits, for performance reasons, the latest file on each commit is padded
                // to be page aligned. The get VBCC chunk request doesn't account for that since the padding is done by the cache
                // at population time and the blob is padded later on.
                is(equalTo(bytesReadFromBlobStore.get() + bytesReadFromIndexingNode.get() + bccTotalPaddingInBytes))
            )
        );

        if (prefetchNonUploadedCommits) {
            // If we prefetch all the commits through the indexing node, the cache would align writes
            // (even thought the latest file in the BCC won't have padding in the final blob uploaded to the blob store).
            assertBusy(
                () -> assertThat(searchEngine.getTotalPrefetchedBytes(), is(lessThanOrEqualTo(toPageAlignedSize(bccTotalSizeInBytes))))
            );
        } else {
            assertBusy(() -> assertThat(searchEngine.getTotalPrefetchedBytes(), is(lessThanOrEqualTo(bccTotalSizeInBytes))));
        }

        if (prefetchNonUploadedCommits) {
            assertThat(bytesReadFromBlobStore.get(), is(equalTo(0L)));
        } else {
            assertThat(bytesReadFromBlobStore.get(), is(greaterThan(0L)));
        }

        var bytesReadFromIndexingNodeBeforeSearch = bytesReadFromIndexingNode.get();
        var bytesReadFromBlobStoreBeforeSearch = bytesReadFromBlobStore.get();

        var searchRequest = prepareSearch(indexName);
        if (randomBoolean()) {
            searchRequest.setQuery(new MatchAllQueryBuilder()).setSize(randomIntBetween(100, 10_000));
        } else {
            searchRequest.setQuery(new TermQueryBuilder("field", "non-existent"));
        }
        assertNoFailures(searchRequest);

        assertThat(bytesReadFromIndexingNode.get(), is(equalTo(bytesReadFromIndexingNodeBeforeSearch)));
        assertThat(bytesReadFromBlobStore.get(), is(equalTo(bytesReadFromBlobStoreBeforeSearch)));
    }

    public void testOnNonUploadedCommitNotificationsTryToPrefetchUploadedData() throws Exception {
        var nodeSettings = Settings.builder()
            .put(SearchCommitPrefetcher.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), false)
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNode = startSearchNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var shardId = new ShardId(resolveIndex(indexName), 0);

        var latestCommitGeneration = client().admin().indices().prepareStats(indexName).get().getAt(0).getCommitStats().getGeneration();
        var vBCCGen = latestCommitGeneration + 1;
        var bccBlobName = BatchedCompoundCommit.blobNameFromGeneration(vBCCGen);

        var bytesReadFromBlobStore = meterBlobStoreReadsForBCC(searchNode, bccBlobName);
        var bytesReadFromIndexingNode = meterIndexingNodeReadsForBCC(indexNode, shardId, vBCCGen);

        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(0L)));

        var numberOfCommits = randomIntBetween(5, 10);
        for (int j = 0; j < numberOfCommits; j++) {
            // Index enough documents so the initial read happening during refresh doesn't include the complete Lucene files
            indexDocs(indexName, 10_000);
            refresh(indexName);
        }
        var currentVirtualBcc = internalCluster().getInstance(StatelessCommitService.class, indexNode).getCurrentVirtualBcc(shardId);
        var bccTotalPaddingInBytes = currentVirtualBcc.getTotalPaddingInBytes();
        var bccTotalSizeInBytes = currentVirtualBcc.getTotalSizeInBytes();

        var uploadCommitNotificationReceived = new CountDownLatch(1);
        AtomicReference<CheckedRunnable<Exception>> pendingNewCommitNotificationHandlerRef = new AtomicReference<>();
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                var newCommitNotification = (NewCommitNotificationRequest) request;
                if (newCommitNotification.isUploaded() && newCommitNotification.getBatchedCompoundCommitGeneration() == vBCCGen) {
                    uploadCommitNotificationReceived.countDown();
                    pendingNewCommitNotificationHandlerRef.set(() -> handler.messageReceived(request, channel, task));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        flush(indexName);

        safeAwait(uploadCommitNotificationReceived);

        indexDocs(indexName, 100);
        refresh(indexName);

        assertBusy(
            () -> assertThat(
                bccTotalSizeInBytes,
                // A BCC can contain multiple Lucene commits, for performance reasons, the latest file on each commit is padded
                // to be page aligned. The get VBCC chunk request doesn't account for that since the padding is done by the cache
                // at population time and the blob is padded later on.
                is(equalTo(bytesReadFromBlobStore.get() + bytesReadFromIndexingNode.get() + bccTotalPaddingInBytes))
            )
        );
        assertBusy(() -> assertThat(searchEngine.getTotalPrefetchedBytes(), is(lessThanOrEqualTo(bccTotalSizeInBytes))));

        var bytesReadFromBlobStoreAfterRefresh = bytesReadFromBlobStore.get();
        assertThat(bytesReadFromBlobStoreAfterRefresh, is(greaterThan(0L)));

        CheckedRunnable<Exception> pendingNewCommitNotificationHandler = pendingNewCommitNotificationHandlerRef.get();
        assertThat(pendingNewCommitNotificationHandler, is(notNullValue()));
        pendingNewCommitNotificationHandler.run();

        var bytesReadFromIndexingNodeBeforeSearch = bytesReadFromIndexingNode.get();
        var bytesReadFromBlobStoreBeforeSearch = bytesReadFromBlobStore.get();

        var searchRequest = prepareSearch(indexName);
        if (randomBoolean()) {
            searchRequest.setQuery(new MatchAllQueryBuilder()).setSize(randomIntBetween(100, 10_000));
        } else {
            searchRequest.setQuery(new TermQueryBuilder("field", "non-existent"));
        }
        assertNoFailures(searchRequest);

        assertThat(bytesReadFromIndexingNode.get(), is(equalTo(bytesReadFromIndexingNodeBeforeSearch)));
        assertThat(bytesReadFromBlobStore.get(), is(equalTo(bytesReadFromBlobStoreBeforeSearch)));
    }

    public void testCommitPrefetchingInForeground() throws Exception {
        var nodeSettings = Settings.builder()
            .put(SearchCommitPrefetcher.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), true)
            .put(SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING.getKey(), false)
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        startSearchNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var initialCommitGeneration = client().admin().indices().prepareStats(indexName).get().getAt(0).getCommitStats().getGeneration();
        var vBCCGen = initialCommitGeneration + 1;
        var shardId = new ShardId(resolveIndex(indexName), 0);

        var vBCCReadBlockedLatch = new CountDownLatch(1);
        var vBCCReadReceived = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    var getVBCCChunkRequest = (GetVirtualBatchedCompoundCommitChunkRequest) request;
                    if (getVBCCChunkRequest.getShardId().equals(shardId)
                        && getVBCCChunkRequest.getVirtualBatchedCompoundCommitGeneration() == vBCCGen) {
                        vBCCReadReceived.countDown();
                        safeAwait(vBCCReadBlockedLatch);
                    }

                    handler.messageReceived(request, channel, task);
                }
            );

        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(0L)));

        indexDocs(indexName, 10_000);
        var refreshFuture = client().admin().indices().prepareRefresh(indexName).execute();

        safeAwait(vBCCReadReceived);

        var currentVirtualBcc = internalCluster().getInstance(StatelessCommitService.class, indexNode).getCurrentVirtualBcc(shardId);
        var bccTotalSizeInBytes = currentVirtualBcc.getTotalSizeInBytes();

        // Since the prefetch is blocked and running in the foreground the commit hasn't moved forward yet.
        IndexShard indexingNodeShard = findIndexShard(indexName);
        assertBusy(() -> assertThat(indexingNodeShard.commitStats().getGeneration(), is(greaterThan(initialCommitGeneration))));
        assertThat(findSearchShard(indexName).commitStats().getGeneration(), is(equalTo(initialCommitGeneration)));
        assertThat(refreshFuture.isDone(), is(equalTo(false)));
        assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(0L)));

        vBCCReadBlockedLatch.countDown();

        refreshFuture.get();

        // If we prefetch all the commits through the indexing node, the cache would align writes
        // (even thought the latest file in the BCC won't have padding in the final blob uploaded to the blob store).
        assertBusy(() -> assertThat(searchEngine.getTotalPrefetchedBytes(), is(lessThanOrEqualTo(toPageAlignedSize(bccTotalSizeInBytes)))));
        assertThat(findSearchShard(indexName).commitStats().getGeneration(), is(greaterThan(initialCommitGeneration)));
    }

    public void testForceCommitPrefetch() throws Exception {
        var forcePrefetch = randomBoolean();
        var nodeSettings = Settings.builder()
            .put(SearchCommitPrefetcher.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), true)
            .put(SearchCommitPrefetcher.FORCE_PREFETCH_SETTING.getKey(), forcePrefetch)
            // There's a single region in the cache to force evictions (if force = true) in each flush
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .build();
        startMasterAndIndexNode(nodeSettings);
        startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        var initialCommitGeneration = client().admin().indices().prepareStats(indexName).get().getAt(0).getCommitStats().getGeneration();

        var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(0L)));

        // Upon recovery, the search shard would use the first region. Clear the cache to start from scratch.
        client().execute(CLEAR_BLOB_CACHE_ACTION, new ClearBlobCacheNodesRequest()).get();
        var numberOfCommits = randomIntBetween(2, 10);
        for (int j = 0; j < numberOfCommits; j++) {
            indexDocs(indexName, 10);
            flush(indexName);
        }

        var bccBlobs = IndexDirectory.unwrapDirectory(findIndexShard(indexName).store().directory())
            .getBlobStoreCacheDirectory()
            .getBlobContainer(findIndexShard(indexName).getOperationPrimaryTerm())
            .listBlobs(OperationPurpose.INDICES);

        var bccBlobsTotalSizeInBytes = bccBlobs.entrySet()
            .stream()
            // We have to exclude the first BCC (empty) because that one is not a candidate for prefetching,
            // it's the base commit for recovery.
            .filter(entry -> entry.getKey().equals(BatchedCompoundCommit.blobNameFromGeneration(initialCommitGeneration)) == false)
            // We prefetch data from the indexing node and we prefetch page aligned chunks, that's why we need to get the page aligned size.
            .mapToLong(entry -> toPageAlignedSize(entry.getValue().length()))
            .sum();

        if (forcePrefetch) {
            assertBusy(() -> assertThat(searchEngine.getTotalPrefetchedBytes(), is(equalTo(bccBlobsTotalSizeInBytes))));
        } else {
            assertBusy(() -> assertThat(searchEngine.getTotalPrefetchedBytes(), is(lessThan(bccBlobsTotalSizeInBytes))));
        }
    }

    private AtomicLong meterIndexingNodeReadsForBCC(String indexNode, ShardId shardId, long vBCCGenToMeter) {
        var bytesReadFromIndexingNode = new AtomicLong();
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    var getVBCCChunkRequest = (GetVirtualBatchedCompoundCommitChunkRequest) request;
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            var getVBCCChunkResponse = (GetVirtualBatchedCompoundCommitChunkResponse) response;
                            if (getVBCCChunkRequest.getShardId().equals(shardId)
                                && getVBCCChunkRequest.getVirtualBatchedCompoundCommitGeneration() == vBCCGenToMeter) {
                                bytesReadFromIndexingNode.addAndGet(getVBCCChunkResponse.getData().length());
                            }
                            channel.sendResponse(response);
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }
                    }, task);
                }
            );
        return bytesReadFromIndexingNode;
    }

    private AtomicLong meterBlobStoreReadsForBCC(String searchNode, String bccBlobName) {
        var bytesReadFromBlobStore = new AtomicLong();
        setNodeRepositoryStrategy(searchNode, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (blobName.equals(bccBlobName)) {
                    return new FilterInputStream(originalSupplier.get()) {
                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            var bytesRead = super.read(b, off, len);
                            if (bytesRead > 0) {
                                bytesReadFromBlobStore.addAndGet(bytesRead);
                            }
                            return bytesRead;
                        }
                    };

                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });
        return bytesReadFromBlobStore;
    }

    public static final class TestStatelessNoRecoveryPrewarming extends Stateless {

        public TestStatelessNoRecoveryPrewarming(Settings settings) {
            super(settings);
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            // no-op the warming on shard recovery so we do not introduce noise in the testing
            return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings) {
                @Override
                public void warmCacheForShardRecovery(
                    Type type,
                    IndexShard indexShard,
                    StatelessCompoundCommit commit,
                    BlobStoreCacheDirectory directory
                ) {}
            };
        }
    }
}
