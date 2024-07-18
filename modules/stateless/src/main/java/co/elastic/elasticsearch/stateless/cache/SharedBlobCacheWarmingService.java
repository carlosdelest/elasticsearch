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

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.recovery.metering.RecoveryMetricsCollector;
import co.elastic.elasticsearch.stateless.utils.IndexingShardRecoveryComparator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_DELAYED;
import static co.elastic.elasticsearch.stateless.lucene.SearchDirectory.unwrapDirectory;
import static org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput.BUFFER_SIZE;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

public class SharedBlobCacheWarmingService {

    public static final Setting<Boolean> STATELESS_BLOB_CACHE_WARMING_ALLOW_FETCH_FROM_INDEXING = Setting.boolSetting(
        "stateless.blob_cache_warming.allow_fetch_from_indexing",
        STATELESS_UPLOAD_DELAYED,
        Setting.Property.NodeScope
    );

    public static final String BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC = "es.blob_cache_warming.page_aligned_bytes.total";

    /** Region of a blob **/
    private record BlobRegion(BlobFile blob, int region) {}

    /** Range of a blob to warm in cache, with a listener to complete once it is warmed **/
    private record BlobRange(String fileName, BlobLocation blobLocation, long position, long length, ActionListener<Void> listener)
        implements
            Comparable<BlobRange> {

        /**
         * Ranges are ordered by decreasing positions in order to fetch them backwards: when fetched from indexing shards, ranges are
         * rounded down more aggressively. By ordering them in backward order, we try to avoid small page aligned forward reads.
         **/
        private static final Comparator<BlobRange> COMPARATOR = Comparator.comparingLong(BlobRange::position).reversed();

        @Override
        public int compareTo(BlobRange other) {
            return COMPARATOR.compare(this, other);
        }
    }

    /** Queue of ranges to warm for a blob region **/
    private static class BlobRangesQueue {

        private final BlobRegion blobRegion;
        private final PriorityBlockingQueue<BlobRange> queue = new PriorityBlockingQueue<>();
        private final AtomicInteger counter = new AtomicInteger();
        private final AtomicLong maxBlobLength = new AtomicLong();

        BlobRangesQueue(BlobRegion blobRegion) {
            this.blobRegion = Objects.requireNonNull(blobRegion);
        }

        /**
         * Adds a range to warm in cache for the current blob region, returning {@code true} if a warming task must be created to warm the
         * range.
         *
         * @param fileName      the name of the file for which the range must be warmed up in cache.
         * @param blobLocation  the blob location of the file
         * @param position      the position in the blob where warming must start
         * @param length        the length of bytes to warm
         * @param listener      the listener to complete once the range is warmed
         * @return {@code true} if a warming task must be created to warm the range, {@code false} otherwise
         */
        private boolean add(String fileName, BlobLocation blobLocation, long position, long length, ActionListener<Void> listener) {
            maxBlobLength.accumulateAndGet(blobLocation.offset() + blobLocation.fileLength(), Math::max);
            queue.add(new BlobRange(fileName, blobLocation, position, length, listener));
            return counter.incrementAndGet() == 1;
        }
    }

    private static final Logger logger = LogManager.getLogger(SharedBlobCacheWarmingService.class);

    private final StatelessSharedBlobCacheService cacheService;
    private final ThreadPool threadPool;
    private final Executor fetchExecutor;
    private final ThrottledTaskRunner throttledTaskRunner;
    private final LongCounter cacheWarmingPageAlignedBytesTotalMetric;
    private final boolean allowFetchFromIndexing;

    public SharedBlobCacheWarmingService(
        StatelessSharedBlobCacheService cacheService,
        ThreadPool threadPool,
        TelemetryProvider telemetryProvider,
        Settings settings
    ) {
        this.cacheService = cacheService;
        this.threadPool = threadPool;
        this.fetchExecutor = threadPool.executor(Stateless.PREWARM_THREAD_POOL);
        this.allowFetchFromIndexing = STATELESS_BLOB_CACHE_WARMING_ALLOW_FETCH_FROM_INDEXING.get(settings);

        // the PREWARM_THREAD_POOL does the actual work but we want to limit the number of prewarming tasks in flight at once so that each
        // one completes sooner, so we use a ThrottledTaskRunner. The throttle limit is a little more than the threadpool size just to avoid
        // having the PREWARM_THREAD_POOL stall while the next task is being queued up
        this.throttledTaskRunner = new ThrottledTaskRunner(
            "prewarming-cache",
            1 + threadPool.info(Stateless.PREWARM_THREAD_POOL).getMax(),
            threadPool.generic() // TODO should be DIRECT, forks to the fetch pool pretty much straight away, but see ES-8448
        );
        this.cacheWarmingPageAlignedBytesTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter(BLOB_CACHE_WARMING_PAGE_ALIGNED_BYTES_TOTAL_METRIC, "Total bytes warmed in cache", "bytes");
    }

    public void warmCacheBeforeUpload(VirtualBatchedCompoundCommit vbcc, ActionListener<Void> listener) {
        assert vbcc.isFrozen();
        long totalSizeInBytes = vbcc.getTotalSizeInBytes();
        cacheService.maybeFetchRegion(
            new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
            0,
            // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService to
            // fully utilize each region. So we just pass it with a value that cover the current region.
            totalSizeInBytes,
            (channel, channelPos, streamFactory, relativePos, len, progressUpdater) -> {
                assert streamFactory == null : streamFactory;
                try (OutputStream output = new OutputStream() {

                    private final ByteBuffer byteBuffer = writeBuffer.get();
                    private int bytesFlushed = 0;

                    @Override
                    public void write(int b) throws IOException {
                        byteBuffer.put((byte) b);
                        if (byteBuffer.hasRemaining() == false) {
                            doFlush(false);
                        }
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        int toWrite = len;
                        while (toWrite > 0) {
                            int toPut = Math.min(byteBuffer.remaining(), toWrite);
                            byteBuffer.put(b, off + (len - toWrite), toPut);
                            toWrite -= toPut;
                            if (byteBuffer.hasRemaining() == false) {
                                doFlush(false);
                            }
                        }
                    }

                    // We don't override the flush method as we only want to do cache aligned flushes - when the buffer is full or on close.
                    private void doFlush(boolean closeFlush) throws IOException {
                        int position = byteBuffer.position();
                        var bytesCopied = SharedBytes.copyBufferToCacheFileAligned(channel, bytesFlushed + channelPos, byteBuffer);
                        bytesFlushed += bytesCopied;
                        assert closeFlush || bytesCopied == position : bytesCopied + " != " + position;
                        assert closeFlush || position % SharedBytes.PAGE_SIZE == 0;
                        assert position > 0;
                    }

                    @Override
                    public void close() throws IOException {
                        if (byteBuffer.position() > 0) {
                            doFlush(true);
                        }
                        assert byteBuffer.position() == 0;
                        progressUpdater.accept(bytesFlushed);
                    }
                }) {
                    vbcc.getBytesByRange(relativePos, Math.toIntExact(Math.min(len, totalSizeInBytes)), output);
                }
            },
            fetchExecutor,
            listener.map(b -> null)
        );
    }

    /**
     * Warms the cache to optimize cache hits during the recovery of an indexing or search shard. The warming happens concurrently
     * with the recovery and doesn't block it.
     *
     * <p>
     * This method uses the list of files of the recovered commit to identify which region(s) of the compound commit blob are likely to be
     * accessed first. It then tries to fetch every region to write them in cache. Note that regions are fetched completely, ie not only the
     * parts required for accessing one or more files. If the cache is under contention then one or more regions may be skipped and not
     * warmed up. If a region is pending to be written to cache by another thread, the warmer skips the region and starts warming the next
     * one without waiting for the region to be available in cache.
     * </p>
     *
     * @param description a description of which warming this is (to distinguish between the many that may be performed in log messages)
     * @param indexShard the shard to warm in cache
     * @param commit the commit to be recovered
     */
    public void warmCacheForShardRecovery(String description, IndexShard indexShard, StatelessCompoundCommit commit) {
        warmCache(description, indexShard, commit, ActionListener.noop());
    }

    protected void warmCache(String description, IndexShard indexShard, StatelessCompoundCommit commit, ActionListener<Void> listener) {
        final Store store = indexShard.store();
        if (store.isClosing() || store.tryIncRef() == false) {
            listener.onFailure(new AlreadyClosedException("Failed to warm cache for " + indexShard + ", store is closing"));
            return;
        }
        try (var warmer = new Warmer(description, indexShard, commit, ActionListener.runAfter(listener, store::decRef))) {
            warmer.run();
        }
    }

    private static boolean shouldFullyWarmUp(String fileName) {
        var extension = LuceneFilesExtensions.fromFile(fileName);
        return extension == null // segments_N are fully warmed up in cache
            || extension.isMetadata() // metadata files
            || StatelessCommitService.isGenerationalFile(fileName); // generational files
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(() -> {
        assert ThreadPool.assertCurrentThreadPool(Stateless.PREWARM_THREAD_POOL);
        return ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE);
    });

    private class Warmer implements Releasable {

        private final String description;
        private final IndexShard indexShard;
        private final StatelessCompoundCommit commit;
        private final ConcurrentMap<BlobRegion, CacheRegionWarmingTask> tasks;
        private final ConcurrentMap<BlobRegion, BlobRangesQueue> queues; // used when stateless upload delayed is enabled
        private final RefCountingListener listeners;

        private final AtomicLong tasksCount = new AtomicLong(0L);
        private final AtomicLong totalBytesCopied = new AtomicLong(0L);

        Warmer(String description, IndexShard indexShard, StatelessCompoundCommit commit, ActionListener<Void> listener) {
            this.description = description;
            this.indexShard = indexShard;
            this.commit = commit;
            this.tasks = new ConcurrentHashMap<>();
            this.queues = new ConcurrentHashMap<>();
            this.listeners = new RefCountingListener(metering(logging(listener)));
        }

        private ActionListener<Void> logging(ActionListener<Void> target) {
            final long started = threadPool.rawRelativeTimeInMillis();
            logger.debug("{} {} warming, generation={}", indexShard.shardId(), description, commit.generation());
            return ActionListener.runBefore(target, () -> {
                final long duration = threadPool.rawRelativeTimeInMillis() - started;
                logger.log(
                    duration >= 5000 ? Level.WARN : Level.DEBUG,
                    "{} {} warming completed in {} ms ({} segments, {} files, {} tasks, {} bytes)",
                    indexShard.shardId(),
                    description,
                    duration,
                    commit.commitFiles()
                        .keySet()
                        .stream()
                        .filter(file -> LuceneFilesExtensions.fromFile(file) == LuceneFilesExtensions.SI)
                        .count(),
                    commit.commitFiles().size(),
                    tasksCount.get(),
                    totalBytesCopied.get()
                );
            });
        }

        private ActionListener<Void> metering(ActionListener<Void> target) {
            return ActionListener.runAfter(
                target,
                () -> cacheWarmingPageAlignedBytesTotalMetric.incrementBy(
                    totalBytesCopied.get(),
                    RecoveryMetricsCollector.commonMetricLabels(indexShard)
                )
            );
        }

        void run() {
            commit.commitFiles()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()))
                .forEach(entry -> addFile(entry.getKey(), entry.getValue()));
        }

        @Override
        public void close() {
            listeners.close();
        }

        /**
         * Finds and scheduled the regions of the compound commit blob that must be warmed up for the given file.
         * <p>
         * The regions to warm are:
         * - the region containing the file header (but the size of the header is unknown so 1024 bytes will be requested)
         * - the region containing the file footer (usually 16 bytes)
         * If the file is a Lucene metadata file or is less than 1024 bytes then it is fully requested to compute the region(s).
         * Additionally this detects and warms the CFE entries
         * </p>
         * @param fileName the name of the Lucene file
         * @param blobLocation the blob location of the Lucene file
         */
        private void addFile(String fileName, BlobLocation blobLocation) {
            if (indexShard.store().isClosing()) {
                // skipping scheduling when store is closing
            } else if (LuceneFilesExtensions.fromFile(fileName) == LuceneFilesExtensions.CFE) {
                SubscribableListener
                    // warm entire CFE file
                    .<Void>newForked(listener -> addLocation(blobLocation, fileName, listener))
                    // parse it and schedule warming of corresponding parts of CFS file
                    .andThenAccept(ignored -> addCfe(fileName))
                    .addListener(listeners.acquire());
            } else if (shouldFullyWarmUp(fileName) || blobLocation.fileLength() <= BUFFER_SIZE) {
                // warm entire file when it is small
                addLocation(blobLocation, fileName, listeners.acquire());
            } else {
                // header
                addLocation(blobLocation, fileName, blobLocation.offset(), BUFFER_SIZE, listeners.acquire());
                // footer
                addLocation(
                    blobLocation,
                    fileName,
                    blobLocation.offset() + blobLocation.fileLength() - CodecUtil.footerLength(),
                    CodecUtil.footerLength(),
                    listeners.acquire()
                );
            }
        }

        private void addLocation(BlobLocation location, String fileName, ActionListener<Void> listener) {
            addLocation(location, fileName, location.offset(), location.fileLength(), listener);
        }

        private void addLocation(BlobLocation location, String fileName, long position, long length, ActionListener<Void> listener) {
            final long start = position;
            final long end = position + length;
            final int regionSize = cacheService.getRegionSize();
            final int startRegion = (int) (start / regionSize);
            final int endRegion = (int) ((end - (end % regionSize == 0 ? 1 : 0)) / regionSize);

            if (startRegion == endRegion) {
                if (allowFetchFromIndexing) {
                    enqueue(new BlobRegion(location.blobFile(), startRegion), fileName, location, position, length, listener);
                } else {
                    addRegion(new BlobRegion(location.blobFile(), startRegion), fileName, listener);
                }
            } else {
                try (var listeners = new RefCountingListener(listener)) {
                    for (int r = startRegion; r <= endRegion; r++) {
                        if (allowFetchFromIndexing) {
                            // adjust the position & length to the region
                            var range = ByteRange.of(Math.max(start, (long) r * regionSize), Math.min(end, (r + 1L) * regionSize));
                            enqueue(
                                new BlobRegion(location.blobFile(), r),
                                fileName,
                                location,
                                range.start(),
                                range.length(),
                                listeners.acquire()
                            );
                        } else {
                            addRegion(new BlobRegion(location.blobFile(), r), fileName, listeners.acquire());
                        }
                    }
                }
            }
        }

        private void addRegion(BlobRegion region, String fileName, ActionListener<Void> listener) {
            var task = tasks.computeIfAbsent(region, k -> {
                var t = new CacheRegionWarmingTask(description, indexShard, region, totalBytesCopied::addAndGet);
                throttledTaskRunner.enqueueTask(t);
                tasksCount.incrementAndGet();
                return t;
            });
            task.files.add(fileName);
            task.listener.addListener(listener);
        }

        private void addCfe(String fileName) {
            assert indexShard.store().hasReferences();// store.incRef() is held by toplevel warmCache until warming is complete
            ActionListener.completeWith(listeners.acquire(), () -> {
                try (var in = indexShard.store().directory().openInput(fileName, IOContext.READONCE)) {
                    var entries = Lucene90CompoundEntriesReader.readEntries(in);

                    var cfs = fileName.replace(".cfe", ".cfs");
                    var cfsLocation = commit.commitFiles().get(cfs);

                    entries.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByKey(new IndexingShardRecoveryComparator()))
                        .forEach(
                            entry -> addFile(
                                entry.getKey(),
                                new BlobLocation(
                                    cfsLocation.blobFile(),
                                    cfsLocation.offset() + entry.getValue().offset(),
                                    entry.getValue().length()
                                )
                            )
                        );
                }
                return null;
            });
        }

        private void enqueue(
            BlobRegion blobRegion,
            String fileName,
            BlobLocation blobLocation,
            long position,
            long length,
            ActionListener<Void> listener
        ) {
            assert allowFetchFromIndexing
                : "method should only be called when " + STATELESS_BLOB_CACHE_WARMING_ALLOW_FETCH_FROM_INDEXING.getKey() + " is true";
            var blobRanges = queues.computeIfAbsent(blobRegion, BlobRangesQueue::new);
            if (blobRanges.add(fileName, blobLocation, position, length, listener)) {
                createWarmingTask(blobRanges);
            }
        }

        private void createWarmingTask(BlobRangesQueue queue) {
            throttledTaskRunner.enqueueTask(new WarmingTask(queue));
            tasksCount.incrementAndGet();
        }

        private boolean isCancelled() {
            return indexShard.store().isClosing() || indexShard.state() != IndexShardState.RECOVERING;
        }

        /**
         * Warms in cache all pending file locations of a given blob region.
         */
        private class WarmingTask implements ActionListener<Releasable> {

            private final BlobRangesQueue queue;
            private final BlobRegion blobRegion;

            WarmingTask(BlobRangesQueue queue) {
                this.queue = Objects.requireNonNull(queue);
                this.blobRegion = queue.blobRegion;
                logger.trace("{} {}: scheduled {}", indexShard.shardId(), description, blobRegion);
            }

            @Override
            public void onResponse(Releasable releasable) {
                try (RefCountingRunnable refs = new RefCountingRunnable(() -> Releasables.close(releasable))) {
                    var cacheKey = new FileCacheKey(indexShard.shardId(), blobRegion.blob.primaryTerm(), blobRegion.blob.blobName());
                    var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());

                    var remaining = queue.counter.get();
                    assert 0 < remaining : remaining;

                    while (0 < remaining) {
                        for (int i = 0; i < remaining; i++) {
                            var item = queue.queue.poll();
                            assert item != null;

                            if (isCancelled()) {
                                item.listener().onResponse(null);
                                continue;
                            }

                            var blobLocation = item.blobLocation();
                            var cacheBlobReader = searchDirectory.getCacheBlobReaderForWarming(blobLocation);
                            // compute the range to warm in cache
                            var range = cacheBlobReader.getRange(
                                item.position(),
                                Math.toIntExact(item.length()),
                                queue.maxBlobLength.get() - item.position()
                            );
                            cacheService.maybeFetchRange(
                                cacheKey,
                                blobRegion.region,
                                range,
                                // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService
                                // to fully utilize each region. So we just pass it with a value that cover the current region.
                                (long) (blobRegion.region + 1) * cacheService.getRegionSize(),
                                (channel, channelPos, streamFactory, relativePos, length, progressUpdater) -> {
                                    // TODO: ES-8987 We should leverage streamFactory to fill multiple gaps with a single request
                                    assert streamFactory == null : streamFactory;
                                    long position = range.start() + relativePos;
                                    try (var in = cacheBlobReader.getRangeInputStream(position, length)) {
                                        assert ThreadPool.assertCurrentThreadPool(Stateless.PREWARM_THREAD_POOL);
                                        var bytesCopied = SharedBytes.copyToCacheFileAligned(
                                            channel,
                                            in,
                                            channelPos,
                                            progressUpdater,
                                            writeBuffer.get().clear()
                                        );
                                        totalBytesCopied.addAndGet(bytesCopied);
                                    }
                                },
                                fetchExecutor,
                                ActionListener.releaseAfter(item.listener().map(ignored -> null), refs.acquire())
                            );
                        }

                        remaining = queue.counter.addAndGet(-remaining);
                        assert 0 <= remaining : remaining;
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> format("%s %s failed to warm region %s", indexShard.shardId(), description, blobRegion), e);
            }
        }
    }

    /**
     * Fetch and write in cache a given region of a compound commit blob file.
     */
    private class CacheRegionWarmingTask implements ActionListener<Releasable> {

        private final String description;
        private final IndexShard indexShard;
        private final BlobRegion target;
        private final SubscribableListener<Void> listener = new SubscribableListener<>();
        private final Set<String> files = ConcurrentCollections.newConcurrentSet();
        private final AtomicLong size = new AtomicLong(0);
        private final LongConsumer totalBytesCopied;

        CacheRegionWarmingTask(String description, IndexShard indexShard, BlobRegion target, LongConsumer totalBytesCopied) {
            this.description = description;
            this.indexShard = indexShard;
            this.target = target;
            this.totalBytesCopied = totalBytesCopied;
            logger.trace("{} {}: scheduled {}", indexShard.shardId(), description, target);
        }

        private boolean shouldWarmRegion() {
            return indexShard.store().isClosing() == false && indexShard.state() == IndexShardState.RECOVERING;
        }

        @Override
        public void onResponse(Releasable releasable) {
            boolean success = false;
            try {
                if (shouldWarmRegion()) {
                    cacheService.maybeFetchRegion(
                        new FileCacheKey(indexShard.shardId(), target.blob.primaryTerm(), target.blob.blobName()),
                        target.region,
                        // this length is not used since we overload computeCacheFileRegionSize in StatelessSharedBlobCacheService to
                        // fully utilize each region. So we just pass it with a value that cover the current region.
                        (long) (target.region + 1) * cacheService.getRegionSize(),
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater) -> {
                            assert streamFactory == null : streamFactory;
                            long position = (long) target.region * cacheService.getRegionSize() + relativePos;
                            var blobContainer = unwrapDirectory(indexShard.store().directory()).getBlobContainer(target.blob.primaryTerm());
                            try (var in = blobContainer.readBlob(OperationPurpose.INDICES, target.blob.blobName(), position, length)) {
                                assert ThreadPool.assertCurrentThreadPool(Stateless.PREWARM_THREAD_POOL);
                                int bytesCopied = SharedBytes.copyToCacheFileAligned(
                                    channel,
                                    in,
                                    channelPos,
                                    progressUpdater,
                                    writeBuffer.get().clear()
                                );
                                size.addAndGet(bytesCopied);
                                totalBytesCopied.accept(bytesCopied);
                            }
                        },
                        fetchExecutor,
                        ActionListener.releaseAfter(listener.map(warmed -> {
                            logger.trace("{} {}: warmed {} with result {}", indexShard.shardId(), description, target, warmed);
                            return null;
                        }), releasable)
                    );
                    success = true;
                } else {
                    listener.onResponse(null);
                }
            } finally {
                if (success == false) {
                    releasable.close();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public String toString() {
            return "CacheRegionWarmingTask{target=" + target + ", files=" + files + ", size=" + size.get() + '}';
        }
    }
}
