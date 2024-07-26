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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.isGenerationalFile;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;

public class SearchDirectory extends BlobStoreCacheDirectory {

    private static final Logger logger = LogManager.getLogger(SearchDirectory.class);

    private final CacheBlobReaderService cacheBlobReaderService;
    private final LongAdder totalBytesReadFromIndexing = new LongAdder();
    private final LongAdder totalBytesWarmedFromIndexing = new LongAdder();

    private final AtomicReference<StatelessCompoundCommit> currentCommit = new AtomicReference<>(null);
    private final MutableObjectStoreUploadTracker objectStoreUploadTracker;

    /**
     * Flag to indicate if opened Lucene generational files must be tracked or not.
     */
    private final boolean trackGenerationalFiles;

    /**
     * Map of terms/generations that are currently in use by opened Lucene generational files.
     * (only populated when {@link #trackGenerationalFiles} is enabled)
     */
    private final Map<PrimaryTermAndGeneration, RefCounted> generationalFilesTermAndGens;

    /**
     * Term/generation of the latest updated commit if it contained at least one generational file.
     */
    private volatile Releasable lastAcquiredGenerationalFilesTermAndGen = null;

    public SearchDirectory(
        StatelessSharedBlobCacheService cacheService,
        CacheBlobReaderService cacheBlobReaderService,
        MutableObjectStoreUploadTracker objectStoreUploadTracker,
        boolean trackGenerationalFiles,
        ShardId shardId
    ) {
        super(cacheService, shardId);
        this.cacheBlobReaderService = cacheBlobReaderService;
        this.objectStoreUploadTracker = objectStoreUploadTracker;
        this.trackGenerationalFiles = trackGenerationalFiles;
        this.generationalFilesTermAndGens = trackGenerationalFiles ? new HashMap<>() : Map.of();
    }

    public void updateLatestUploadInfo(
        PrimaryTermAndGeneration latestUploadedBccTermAndGen,
        PrimaryTermAndGeneration ccTermAndGen,
        String nodeId
    ) {
        objectStoreUploadTracker.updateLatestUploadInfo(latestUploadedBccTermAndGen, ccTermAndGen, nodeId);
    }

    private Releasable acquireGenerationalFileTermAndGeneration(PrimaryTermAndGeneration termAndGen, String name) {
        assert trackGenerationalFiles;
        synchronized (generationalFilesTermAndGens) {
            var refCounted = generationalFilesTermAndGens.get(termAndGen);
            if (refCounted == null || refCounted.tryIncRef() == false) {
                throw new IllegalStateException("Cannot acquire " + termAndGen + " for generational file [" + name + ']');
            }
            assert generationalFilesTermAndGens.isEmpty() == false;
            return refCounted::decRef;
        }
    }

    private Releasable addGenerationalFileTermAndGeneration(PrimaryTermAndGeneration termAndGen) {
        assert trackGenerationalFiles;
        RefCounted refCounted;
        synchronized (generationalFilesTermAndGens) {
            refCounted = generationalFilesTermAndGens.get(termAndGen);
            if (refCounted == null) {
                refCounted = AbstractRefCounted.of(() -> removeGenerationalFileTermAndGeneration(termAndGen));
                generationalFilesTermAndGens.put(termAndGen, refCounted);
            } else {
                // when updating the commit we always acquire the BCC term/gen the commit is part of; if two commits are in the same BCC
                // the second update need to incRef the same BCC term/gen instead of creating a new AbstractRefCounted
                refCounted.incRef();
            }
        }
        return refCounted::decRef;
    }

    private void removeGenerationalFileTermAndGeneration(PrimaryTermAndGeneration termAndGen) {
        assert trackGenerationalFiles;
        synchronized (generationalFilesTermAndGens) {
            var removed = generationalFilesTermAndGens.remove(termAndGen);
            assert removed != null : termAndGen;
            assert removed.hasReferences() == false : termAndGen;
        }
    }

    /**
     * @return the set of {@link PrimaryTermAndGeneration} used by opened Lucene generational files
     */
    public Set<PrimaryTermAndGeneration> getAcquiredGenerationalFileTermAndGenerations() {
        if (trackGenerationalFiles) {
            synchronized (generationalFilesTermAndGens) {
                return Set.copyOf(generationalFilesTermAndGens.keySet());
            }
        }
        return Set.of();
    }

    @Override
    protected IndexInput doOpenInput(String name, IOContext context, BlobLocation blobLocation) {
        if (trackGenerationalFiles == false || isGenerationalFile(name) == false) {
            return super.doOpenInput(name, context, blobLocation);
        }
        var releasable = acquireGenerationalFileTermAndGeneration(blobLocation.getBatchedCompoundCommitTermAndGeneration(), name);
        return doOpenInput(name, context, blobLocation, releasable);
    }

    /**
     * Moves the directory to a new commit by setting the newly valid map of files and their metadata.
     *
     * @param newCommit map of file name to store metadata
     * @return true if this update advanced the commit tracked by this directory
     */
    public boolean updateCommit(StatelessCompoundCommit newCommit) {
        assert blobContainer.get() != null : shardId + " must have the blob container set before any commit update";
        assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());

        var previousGenerationalFilesTermAndGen = trackGenerationalFiles ? this.lastAcquiredGenerationalFilesTermAndGen : null;
        try {
            final var updatedMetadata = new HashMap<>(currentMetadata);
            PrimaryTermAndGeneration generationalFilesTermAndGen = null;
            long commitSize = 0L;
            for (var entry : newCommit.commitFiles().entrySet()) {
                var fileName = entry.getKey();
                var blobLocation = entry.getValue();
                if (trackGenerationalFiles && isGenerationalFile(fileName)) {
                    // blob locations for generational files are not updated: we pin the file to the first blob location that we know about.
                    // we expect generational files to be opened when the reader is refreshed and picks up the generational files for the
                    // first time and never reopened them after that (as segment core readers are handed over between refreshed reader
                    // instances).
                    updatedMetadata.putIfAbsent(fileName, blobLocation);
                    if (generationalFilesTermAndGen == null) {
                        generationalFilesTermAndGen = blobLocation.getBatchedCompoundCommitTermAndGeneration();
                    }
                    assert blobLocation.getBatchedCompoundCommitTermAndGeneration().equals(generationalFilesTermAndGen)
                        : "Because they are either new or copied, generational files should all belong to the same BCC, but "
                            + fileName
                            + " has location "
                            + blobLocation
                            + " which is different from "
                            + generationalFilesTermAndGen;
                } else {
                    updatedMetadata.put(fileName, blobLocation);
                }
                commitSize += blobLocation.fileLength();
            }
            // If we have generational file(s) in the new commit, we create a ref counted instance that holds the term/generation of the
            // batched compound commit so that it can be reported as used to the indexing shard in new commit responses. The ref counted
            // instance will be decRef on the next commit update or when the directory is closed. Any generational file opened between two
            // commits update should incRef the instance to indicate that the BCC term/generation is in use and decRef it once the file is
            // closed. When fully decRefed, the BCC term/gen is removed from the set of used generations.
            if (generationalFilesTermAndGen != null) {
                var releasable = addGenerationalFileTermAndGeneration(generationalFilesTermAndGen);
                // use releaseOnce to decRef only once, either on commit update or directory close
                this.lastAcquiredGenerationalFilesTermAndGen = Releasables.releaseOnce(releasable);
            } else if (trackGenerationalFiles) {
                // commit has no generational files
                this.lastAcquiredGenerationalFilesTermAndGen = null;
            }
            currentMetadata = Map.copyOf(updatedMetadata);
            currentDataSetSizeInBytes = commitSize;
            // TODO: Commits may not arrive in order. However, the maximum commit we have received is the commit of this directory since the
            // TODO: files always accumulate
            return currentCommit.accumulateAndGet(newCommit, (current, contender) -> {
                if (current == null) {
                    return contender;
                } else if (current.generation() > contender.generation()) {
                    return current;
                } else {
                    return contender;
                }
            }).generation() == newCommit.generation();
        } finally {
            try {
                Releasables.close(previousGenerationalFilesTermAndGen);
            } finally {
                assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
            }
        }
    }

    /**
     * Removes superfluous files
     * @param filesToRetain the files to retain
     */
    public void retainFiles(Set<String> filesToRetain) {
        if (filesToRetain.containsAll(currentMetadata.keySet()) == false) {
            assert assertCompareAndSetUpdatingCommitThread(null, Thread.currentThread());
            try {
                final Map<String, BlobLocation> updated = new HashMap<>(currentMetadata);
                updated.keySet().retainAll(filesToRetain);
                assert updated.keySet().containsAll(filesToRetain)
                    : "missing files [" + Sets.difference(filesToRetain, updated.keySet()) + "]";
                currentMetadata = Map.copyOf(updated);
            } finally {
                assert assertCompareAndSetUpdatingCommitThread(Thread.currentThread(), null);
            }
        }
    }

    /**
     * For test usage only.
     */
    @Override
    StatelessSharedBlobCacheService getCacheService() {
        return super.getCacheService();
    }

    // TODO this method works because we never prune old commits files
    public OptionalLong getPrimaryTerm(String segmentsFileName) throws FileNotFoundException {
        final BlobLocation location = getBlobLocation(segmentsFileName);
        if (location != null) {
            return OptionalLong.of(location.primaryTerm());
        }
        if (segmentsFileName.equals(EmptyDirectory.INSTANCE.getSegmentsFileName())) {
            return OptionalLong.empty();
        }
        var exception = new FileNotFoundException(segmentsFileName);
        assert false : exception;
        throw exception;
    }

    public StatelessCompoundCommit getCurrentCommit() {
        // Only used to initialize the search engine
        return currentCommit.get();
    }

    @Override
    public CacheBlobReader getCacheBlobReader(BlobLocation blobLocation) {
        return cacheBlobReaderService.getCacheBlobReader(
            shardId,
            this::getBlobContainer,
            blobLocation,
            objectStoreUploadTracker,
            totalBytesReadFromObjectStore::add,
            totalBytesReadFromIndexing::add
        );
    }

    @Override
    public CacheBlobReader getCacheBlobReaderForWarming(BlobLocation blobLocation) {
        return cacheBlobReaderService.getCacheBlobReader(
            shardId,
            this::getBlobContainer,
            blobLocation,
            objectStoreUploadTracker,
            totalBytesWarmedFromObjectStore::add,
            totalBytesWarmedFromIndexing::add
        );
    }

    public long totalBytesReadFromIndexing() {
        return totalBytesReadFromIndexing.sum();
    }

    public long totalBytesWarmedFromIndexing() {
        return totalBytesWarmedFromIndexing.sum();
    }

    public static SearchDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof SearchDirectory searchDirectory) {
                return searchDirectory;
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + SearchDirectory.class);
        assert false : e;
        throw e;
    }

    public void downloadCommit(StatelessCompoundCommit commit, Executor fetchExecutor, ActionListener<Void> listener) {
        assert false : "this method requires fixes, see https://elasticco.atlassian.net/browse/ES-8140";
        try (RefCountingListener refCountingListener = new RefCountingListener(listener)) {
            final String latestCompoundCommitLocation = StatelessCompoundCommit.blobNameFromGeneration(commit.generation());
            commit.commitFiles().forEach((file, blobLocation) -> {
                if (blobLocation.blobName().equals(latestCompoundCommitLocation) == false) {
                    // only pre-fetch files that are part of the latest commit generation
                    return;
                }
                FileCacheKey key = new FileCacheKey(shardId, blobLocation.primaryTerm(), blobLocation.blobName());
                final var blobLength = Long.MIN_VALUE;
                final var container = getBlobContainer(blobLocation.primaryTerm());
                cacheService.maybeFetchFullEntry(
                    key,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> ActionListener
                        .completeWith(completionListener, () -> {
                            assert streamFactory == null : streamFactory;
                            final ByteRange rangeToWrite = BlobCacheUtils.computeRange(
                                cacheService.getRangeSize(),
                                relativePos,
                                length,
                                blobLength
                            );
                            final long streamStartPosition = rangeToWrite.start() + relativePos;
                            try (
                                InputStream in = container.readBlob(OperationPurpose.INDICES, key.fileName(), streamStartPosition, length)
                            ) {
                                // assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
                                logger.trace(
                                    "{}: writing channel {} pos {} length {} (details: {})",
                                    key.fileName(),
                                    channelPos,
                                    relativePos,
                                    length,
                                    key
                                );
                                SharedBytes.copyToCacheFileAligned(
                                    channel,
                                    in,
                                    channelPos,
                                    relativePos,
                                    length,
                                    progressUpdater,
                                    writeBuffer.get().clear()
                                );
                                return null;
                            }
                        }),
                    fetchExecutor,
                    refCountingListener.acquire()
                );
            });
        }
    }

    @Override
    public void close() throws IOException {
        Releasables.close(lastAcquiredGenerationalFilesTermAndGen);
        if (Assertions.ENABLED) {
            synchronized (generationalFilesTermAndGens) {
                assert generationalFilesTermAndGens.isEmpty() : "expect all inputs to be closed at the time the directory is closed";
            }
        }
        super.close();
    }

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );
}
