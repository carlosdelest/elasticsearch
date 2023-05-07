/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.Strings.format;

public class StatelessCommitService {

    private static final Logger logger = LogManager.getLogger(StatelessCommitService.class);

    private final ObjectStoreService objectStoreService;
    private final Supplier<String> ephemeralNodeIdSupplier;
    private final Function<ShardId, IndexShardRoutingTable> shardRouting;
    private final Client client;
    private final ThreadPool threadPool;
    // We don't do null checks when reading from this sub-map because we hold a commit reference while files are being uploaded. This will
    // prevent commit deletion in the interim.
    private final ConcurrentHashMap<ShardId, ShardCommitState> fileToBlobFile = new ConcurrentHashMap<>();

    public StatelessCommitService(ObjectStoreService objectStoreService, ClusterService clusterService, Client client) {
        this(
            objectStoreService,
            () -> clusterService.localNode().getEphemeralId(),
            (shardId) -> clusterService.state().routingTable().shardRoutingTable(shardId),
            clusterService.threadPool(),
            client
        );
    }

    public StatelessCommitService(
        ObjectStoreService objectStoreService,
        Supplier<String> ephemeralNodeIdSupplier,
        Function<ShardId, IndexShardRoutingTable> shardRouting,
        ThreadPool threadPool,
        Client client
    ) {
        this.objectStoreService = objectStoreService;
        this.ephemeralNodeIdSupplier = ephemeralNodeIdSupplier;
        this.shardRouting = shardRouting;
        this.threadPool = threadPool;
        this.client = client;
    }

    public void markCommitDeleted(ShardId shardId, Collection<String> commitFiles) {
        ShardCommitState commitState = getSafe(fileToBlobFile, shardId);
        commitState.markCommitDeleted(commitFiles);
    }

    public void onCommitCreation(StatelessCommitRef reference) {
        logger.debug("{} uploading commit [{}][{}]", reference.getShardId(), reference.getSegmentsFileName(), reference.getGeneration());

        ShardCommitState commitState = getSafe(fileToBlobFile, reference.getShardId());
        commitState.markNewCommit(reference.getCommitFiles(), reference.getAdditionalFiles());
        CommitUpload commitUpload = new CommitUpload(commitState, ActionListener.wrap(new ActionListener<>() {
            @Override
            public void onResponse(StatelessCompoundCommit commit) {
                NewCommitNotificationRequest request = new NewCommitNotificationRequest(shardRouting.apply(commit.shardId()), commit);
                client.execute(TransportNewCommitNotificationAction.TYPE, request);
            }

            @Override
            public void onFailure(Exception e) {
                assert commitState.isClosed || e instanceof EsRejectedExecutionException;
                logger.warn(
                    () -> format(
                        "%s failed to upload commit [%s] to object store because shard was closed",
                        reference.getShardId(),
                        reference.getGeneration()
                    ),
                    e
                );
            }
        }), reference, TimeValue.timeValueMillis(50));
        commitUpload.run();

    }

    public class CommitUpload extends RetryableAction<StatelessCompoundCommit> {

        private final StatelessCommitRef reference;
        private final ShardCommitState shardCommitState;
        private final ShardId shardId;
        private final long generation;
        private final long startNanos;
        private final AtomicLong uploadedFileCount = new AtomicLong();
        private final AtomicLong uploadedFileBytes = new AtomicLong();
        private final AtomicReference<Map<String, Long>> commitFilesToLength = new AtomicReference<>();

        public CommitUpload(
            ShardCommitState shardCommitState,
            ActionListener<StatelessCompoundCommit> listener,
            StatelessCommitRef reference,
            TimeValue initialDelay
        ) {
            super(
                logger,
                threadPool,
                initialDelay,
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueMillis(Long.MAX_VALUE),
                listener,
                ThreadPool.Names.GENERIC
            );
            this.shardCommitState = shardCommitState;
            this.reference = reference;
            this.shardId = reference.getShardId();
            this.generation = reference.getGeneration();
            this.startNanos = threadPool.relativeTimeInNanos();
        }

        @Override
        public void tryAction(ActionListener<StatelessCompoundCommit> originalListener) {
            ActionListener<StatelessCompoundCommit> listener = originalListener.delegateResponse((l, e) -> {
                logger.info(() -> format("%s failed attempt to upload commit [%s] to object store, will retry", shardId, generation), e);
                l.onFailure(e);
            });

            try {
                // Only do this once across multiple retries since file lengths should not change
                if (this.commitFilesToLength.get() == null) {
                    final Collection<String> commitFileNames = reference.getCommitFiles();
                    Map<String, Long> mutableCommitFiles = Maps.newHashMapWithExpectedSize(commitFileNames.size());
                    for (String fileName : commitFileNames) {
                        mutableCommitFiles.put(fileName, reference.getDirectory().fileLength(fileName));
                    }
                    this.commitFilesToLength.set(Collections.unmodifiableMap(mutableCommitFiles));
                }

                ActionListener<Void> missingUploadedListener = listener.delegateFailure((l, v) -> uploadStatelessCommitFile(l));
                ActionListener<Void> additionalUploadedListener = missingUploadedListener.delegateFailure((l, v) -> uploadMissingFiles(l));
                uploadAdditionalFiles(additionalUploadedListener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void uploadAdditionalFiles(ActionListener<Void> listener) {
            // We resolve missing files as it is possible a previous attempt successfully uploaded some files
            final Collection<String> additionalFiles = reference.getAdditionalFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .filter(file -> shardCommitState.fileMap.get(file).isUploaded() == false)
                .collect(Collectors.toList());

            logger.trace("{} uploading [{}] additional files for commit [{}]", shardId, additionalFiles.size(), generation);

            uploadFiles(additionalFiles, listener);
        }

        private void uploadMissingFiles(ActionListener<Void> listener) {
            final List<String> missingFiles = reference.getCommitFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .filter(f -> shardCommitState.fileMap.get(f).isUploaded() == false)
                .toList();

            logger.trace("{} uploading [{}] missing files for commit [{}]", shardId, missingFiles.size(), generation);
            uploadFiles(missingFiles, listener);
        }

        private void uploadStatelessCommitFile(ActionListener<StatelessCompoundCommit> listener) {
            String commitFileName = StatelessCompoundCommit.NAME + generation;
            StatelessCompoundCommit.Writer pendingCommit = shardCommitState.returnPendingCompoundCommit(
                shardId,
                generation,
                reference.getPrimaryTerm(),
                commitFilesToLength.get()
            );

            logger.trace("{} uploading stateless commit file [{}] for commit [{}]", shardId, commitFileName, generation);
            objectStoreService.uploadStatelessCommitFile(
                shardId,
                reference.getPrimaryTerm(),
                generation,
                reference.getDirectory(),
                commitFileName,
                startNanos,
                pendingCommit,
                listener.delegateFailure((l, commit) -> {
                    for (String internalFile : pendingCommit.getInternalFiles()) {
                        uploadedFileCount.getAndIncrement();
                        uploadedFileBytes.getAndAdd(commitFilesToLength.get().get(internalFile));
                        shardCommitState.markFileUploaded(internalFile, commit.commitFiles().get(internalFile));
                    }
                    shardCommitState.markCommitUploaded(commit);
                    final long end = threadPool.relativeTimeInNanos();
                    logger.debug(
                        () -> format(
                            "%s commit [%s] uploaded in [%s] ms (%s files, %s total bytes)",
                            shardId,
                            generation,
                            TimeValue.nsecToMSec(end - startNanos),
                            uploadedFileCount.get(),
                            uploadedFileBytes.get()
                        )
                    );
                    l.onResponse(commit);
                })
            );

        }

        private void uploadFiles(Collection<String> files, ActionListener<Void> listener) {
            try (var listeners = new RefCountingListener(ActionListener.wrap(listener))) {
                files.forEach(
                    file -> objectStoreService.uploadCommitFile(
                        shardId,
                        reference.getPrimaryTerm(),
                        generation,
                        reference.getDirectory(),
                        file,
                        startNanos,
                        listeners.acquire(location -> {
                            uploadedFileCount.getAndIncrement();
                            uploadedFileBytes.getAndAdd(commitFilesToLength.get().get(file));
                            shardCommitState.markFileUploaded(file, location);
                        })
                    )
                );
            }

        }

        @Override
        public void onFinished() {
            Releasable releasable = () -> {};
            IOUtils.closeWhileHandlingException(reference, releasable);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return shardCommitState.isClosed == false;
        }
    }

    public void register(ShardId shardId) {
        ShardCommitState existing = fileToBlobFile.put(shardId, new ShardCommitState(shardId));
        assert existing == null : shardId + " already registered";
    }

    public void unregister(ShardId shardId) {
        ShardCommitState removed = fileToBlobFile.remove(shardId);
        assert removed != null : shardId + " not registered";
        removed.close();
    }

    public void addOrNotify(ShardId shardId, long generation, ActionListener<Void> listener) {
        requireNonNull(listener, "listener cannot be null");
        ShardCommitState commitState = getSafe(fileToBlobFile, shardId);
        commitState.addOrNotify(generation, listener);
    }

    // Visible for testing
    Map<String, BlobFile> getFileToBlobFile(ShardId shardId) {
        return fileToBlobFile.get(shardId).fileMap;
    }

    private static ShardCommitState getSafe(ConcurrentHashMap<ShardId, ShardCommitState> map, ShardId shardId) {
        final ShardCommitState commitState = map.get(shardId);
        if (commitState == null) {
            throw new AlreadyClosedException("shard [" + shardId + "] has already been closed");
        }
        return commitState;
    }

    private class ShardCommitState {

        private final ShardId shardId;

        private final Map<String, BlobFile> fileMap = new ConcurrentHashMap<>();
        private List<Tuple<Long, ActionListener<Void>>> generationListeners = null;
        private long generationUploaded = -1;
        private volatile boolean isClosed;

        private ShardCommitState(ShardId shardId) {
            this.shardId = shardId;
        }

        public void markFileUploaded(String name, BlobLocation objectStoreLocation) {
            fileMap.get(name).setBlobLocation(objectStoreLocation);
        }

        public StatelessCompoundCommit.Writer returnPendingCompoundCommit(
            ShardId shardId,
            long generation,
            long primaryTerm,
            Map<String, Long> commitFiles
        ) {
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                shardId,
                generation,
                primaryTerm,
                ephemeralNodeIdSupplier.get()
            );
            for (Map.Entry<String, Long> commitFile : commitFiles.entrySet()) {
                String fileName = commitFile.getKey();
                if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
                    BlobFile blobFile = fileMap.get(fileName);
                    assert blobFile.isUploaded();
                    writer.addReferencedBlobFile(fileName, blobFile.location());
                } else {
                    writer.addInternalFile(fileName, commitFile.getValue());
                }
            }
            return writer;
        }

        public void markNewCommit(Collection<String> commitFiles, Set<String> additionalFiles) {
            for (String file : commitFiles) {
                if (additionalFiles.contains(file)) {
                    BlobFile existing = fileMap.put(file, new BlobFile());
                    assert existing == null;
                } else {
                    fileMap.get(file).incRef();
                }
            }
        }

        public void markCommitDeleted(Collection<String> commitFiles) {
            for (String file : commitFiles) {
                boolean shouldRemove = fileMap.get(file).decRef();
                if (shouldRemove) {
                    fileMap.remove(file);
                }
            }
        }

        public void markCommitUploaded(StatelessCompoundCommit commit) {
            markUploadedGeneration(commit.generation());
        }

        private void markUploadedGeneration(long newGeneration) {
            List<ActionListener<Void>> listenersToFire = null;
            List<Tuple<Long, ActionListener<Void>>> listenersToReregister = null;
            synchronized (this) {
                generationUploaded = Math.max(generationUploaded, newGeneration);

                // No listeners to check or generation did not increase so just bail early
                if (generationListeners == null || generationUploaded != newGeneration) {
                    return;
                }

                for (Tuple<Long, ActionListener<Void>> tuple : generationListeners) {
                    Long generation = tuple.v1();
                    if (generationUploaded >= generation) {
                        if (listenersToFire == null) {
                            listenersToFire = new ArrayList<>();
                        }
                        listenersToFire.add(tuple.v2());
                    } else {
                        if (listenersToReregister == null) {
                            listenersToReregister = new ArrayList<>();
                        }
                        listenersToReregister.add(tuple);
                    }
                }
                generationListeners = listenersToReregister;
            }

            if (listenersToFire != null) {
                ActionListener.onResponse(listenersToFire, null);
            }
        }

        private void addOrNotify(long generation, ActionListener<Void> listener) {
            boolean completeListenerSuccess = false;
            boolean completeListenerClosed = false;
            synchronized (this) {
                if (isClosed) {
                    completeListenerClosed = true;
                } else if (generationUploaded >= generation) {
                    // Location already visible, just call the listener
                    completeListenerSuccess = true;
                } else {
                    List<Tuple<Long, ActionListener<Void>>> listeners = generationListeners;
                    ActionListener<Void> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
                        listener,
                        threadPool.getThreadContext()
                    );
                    if (listeners == null) {
                        listeners = new ArrayList<>();
                    }
                    listeners.add(new Tuple<>(generation, contextPreservingListener));
                    generationListeners = listeners;
                }
            }

            if (completeListenerClosed) {
                listener.onFailure(new AlreadyClosedException("shard [" + shardId + "] has already been closed"));
            } else if (completeListenerSuccess) {
                listener.onResponse(null);
            }
        }

        private void close() {
            List<Tuple<Long, ActionListener<Void>>> listenersToFail;
            synchronized (this) {
                isClosed = true;
                listenersToFail = generationListeners;
                generationListeners = null;
            }

            if (listenersToFail != null) {
                ActionListener.onFailure(
                    listenersToFail.stream().map(Tuple::v2).collect(Collectors.toList()),
                    new AlreadyClosedException("shard closed")
                );
            }
        }
    }

    private static class BlobFile extends AbstractRefCounted {

        private volatile BlobLocation blobLocation;

        private void setBlobLocation(BlobLocation uploadedLocation) {
            if (this.blobLocation == null) {
                this.blobLocation = uploadedLocation;
            }
        }

        private boolean isUploaded() {
            return blobLocation != null;
        }

        private BlobLocation location() {
            return Objects.requireNonNull(blobLocation);
        }

        @Override
        protected void closeInternal() {}
    }
}
