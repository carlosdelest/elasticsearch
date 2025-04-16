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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.action.FetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportFetchShardCommitsInUseAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.recovery.RegisterCommitResponse;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_DURATION_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration.ZERO;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatelessCommitServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private int primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(StatelessCommitServiceTests.class.getName(), Settings.EMPTY);
        primaryTerm = randomIntBetween(1, 100);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCloseIdempotentlyChangesStateAndCompletesListeners() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs), 1)) {

            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(2);

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, commitRef.getGeneration());
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }
            long lastCommitGeneration = commitRefs.get(commitRefs.size() - 1).getGeneration();

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(
                testHarness.shardId,
                lastCommitGeneration + 1,
                ActionListener.assertOnce(future)
            );

            testHarness.commitService.closeShard(testHarness.shardId);

            expectThrows(AlreadyClosedException.class, future::actionGet);

            // Test that close can be called twice
            testHarness.commitService.closeShard(testHarness.shardId);

            // Test listeners immediately completed for closed shard
            PlainActionFuture<Void> future2 = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(
                testHarness.shardId,
                lastCommitGeneration + 2,
                ActionListener.assertOnce(future2)
            );
            expectThrows(AlreadyClosedException.class, future2::actionGet);
        }
    }

    public void testCommitUpload() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            randomlyUseInitializingEmpty(testHarness);

            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(randomIntBetween(3, 8));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();

            Set<PrimaryTermAndGeneration> consumerCalls = Collections.newSetFromMap(new ConcurrentHashMap<>());
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> consumerCalls.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );
            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
                testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, commitRef.getGeneration());
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                var generationalFiles = commitRef.getCommitFiles().stream().filter(StatelessCommitService::isGenerationalFile).toList();
                var internalFiles = returnInternalFiles(testHarness, List.of(blobNameFromGeneration(commitRef.getGeneration())));
                assertThat(generationalFiles, everyItem(is(in(internalFiles))));
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );

            Set<PrimaryTermAndGeneration> expectedConsumerCalls = commitRefs.stream()
                .map(c -> new PrimaryTermAndGeneration(primaryTerm, c.getGeneration()))
                .collect(Collectors.toSet());

            assertThat(consumerCalls, equalTo(expectedConsumerCalls));
        }
    }

    public void testCommitUploadIncludesRetries() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<String> failedFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());

        try (var testHarness = createNode((fileName, runnable) -> {
            if (failedFiles.contains(fileName) == false && randomBoolean()) {
                failedFiles.add(fileName);
                throw new IOException("Failed");
            } else {
                // Rarely fail a second time
                if (failedFiles.contains(fileName) && rarely()) {
                    throw new IOException("Failed");
                } else {
                    runnable.run();
                    uploadedBlobs.add(fileName);
                }
            }

        }, fileCapture(uploadedBlobs))) {

            randomlyUseInitializingEmpty(testHarness);

            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(randomIntBetween(2, 4));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
                // todo: reevaluate the need to upload every commit (this test assume max_commits=1)
                testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, commitRef.getGeneration());
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testSecondCommitDefersSchedulingForFirstCommit() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicReference<String> commitFileToBlock = new AtomicReference<>();
        AtomicReference<String> firstCommitFile = new AtomicReference<>();
        AtomicReference<String> secondCommitFile = new AtomicReference<>();

        CountDownLatch startingUpload = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileCapture = fileCapture(uploadedBlobs);
        try (var testHarness = createNode(fileCapture(uploadedBlobs), (compoundCommitFile, runnable) -> {
            if (compoundCommitFile.equals(firstCommitFile.get())) {
                startingUpload.countDown();
                safeAwait(blocking);
                assertFalse(uploadedBlobs.contains(secondCommitFile.get()));
            } else {
                assertEquals(compoundCommitFile, secondCommitFile.get());
                assertTrue(uploadedBlobs.contains(firstCommitFile.get()));
            }
            compoundCommitFileCapture.accept(compoundCommitFile, runnable);
        }, 1)) {

            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(2);
            StatelessCommitRef firstCommit = commitRefs.get(0);
            StatelessCommitRef secondCommit = commitRefs.get(1);

            commitFileToBlock.set(
                firstCommit.getAdditionalFiles()
                    .stream()
                    .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                    .findFirst()
                    .get()
            );

            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();
            firstCommitFile.set(compoundCommitFiles.get(0));
            secondCommitFile.set(compoundCommitFiles.get(1));

            testHarness.commitService.onCommitCreation(firstCommit);
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, firstCommit.getGeneration());
            safeAwait(startingUpload);
            assertThat(uploadedBlobs, not(hasItems(commitFileToBlock.get())));
            assertThat(uploadedBlobs, not(hasItems(firstCommitFile.get())));

            testHarness.commitService.onCommitCreation(secondCommit);

            assertThat(uploadedBlobs, not(hasItems(secondCommitFile.get())));

            blocking.countDown();

            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testRelocationWaitsForAllPendingCommitsAndDoesNotAllowNew() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicReference<String> commitFileToBlock = new AtomicReference<>();
        AtomicReference<String> firstCommitFile = new AtomicReference<>();
        AtomicReference<String> secondCommitFile = new AtomicReference<>();

        CountDownLatch blockUploads = new CountDownLatch(1);

        try (var testHarness = createNode(fileCapture(uploadedBlobs), (file, runnable) -> {
            safeAwait(blockUploads);
            runnable.run();
            uploadedBlobs.add(file);
        }, 1)) {

            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(3);
            StatelessCommitRef firstCommit = commitRefs.get(0);
            StatelessCommitRef secondCommit = commitRefs.get(1);
            StatelessCommitRef thirdCommit = commitRefs.get(2);

            commitFileToBlock.set(
                firstCommit.getAdditionalFiles()
                    .stream()
                    .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                    .findFirst()
                    .get()
            );

            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();
            firstCommitFile.set(compoundCommitFiles.get(0));
            secondCommitFile.set(compoundCommitFiles.get(1));

            testHarness.commitService.onCommitCreation(firstCommit);
            assertThat(uploadedBlobs, not(hasItems(commitFileToBlock.get())));
            assertThat(uploadedBlobs, not(hasItems(firstCommitFile.get())));

            testHarness.commitService.onCommitCreation(secondCommit);

            assertThat(uploadedBlobs, not(hasItems(secondCommitFile.get())));

            assertThat(testHarness.commitService.getMaxGenerationToUploadForFlush(testHarness.shardId), equalTo(-1L));
            PlainActionFuture<Void> listener = new PlainActionFuture<>();
            ActionListener<Void> relocationListener = testHarness.commitService.markRelocating(testHarness.shardId, 1, listener);
            assertThat(testHarness.commitService.getMaxGenerationToUploadForFlush(testHarness.shardId), equalTo(1L));

            testHarness.commitService.onCommitCreation(thirdCommit);
            PlainActionFuture<Void> thirdCommitListener = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(
                testHarness.shardId,
                thirdCommit.getGeneration(),
                thirdCommitListener
            );

            assertFalse(thirdCommitListener.isDone());
            assertFalse(listener.isDone());

            blockUploads.countDown();
            listener.actionGet();

            relocationListener.onResponse(null);

            assertTrue(thirdCommitListener.isDone());

            expectThrows(UnavailableShardsException.class, thirdCommitListener::actionGet);

            ArrayList<String> uploadedFiles = new ArrayList<>(uploadedBlobs);

            assertThat(
                "Expected that compound commit file " + compoundCommitFiles.get(0) + " had been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItem(compoundCommitFiles.get(0))
            );

            assertThat(
                "Expected that compound commit file " + compoundCommitFiles.get(1) + " had been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItem(compoundCommitFiles.get(1))
            );

            assertThat(
                "Expected that compound commit file " + compoundCommitFiles.get(2) + " had not been uploaded " + uploadedFiles,
                uploadedFiles,
                not(hasItem(compoundCommitFiles.get(2)))
            );
        }
    }

    public void testFailedRelocationAllowsCommitsToContinue() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicReference<String> commitFileToBlock = new AtomicReference<>();
        AtomicReference<String> firstCommitFile = new AtomicReference<>();
        AtomicReference<String> secondCommitFile = new AtomicReference<>();

        CountDownLatch startingUpload = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileCapture = fileCapture(uploadedBlobs);
        try (var testHarness = createNode(fileCapture(uploadedBlobs), (compoundCommitFile, runnable) -> {
            if (compoundCommitFile.equals(firstCommitFile.get())) {
                startingUpload.countDown();
                safeAwait(blocking);
                assertFalse(uploadedBlobs.contains(secondCommitFile.get()));
            }
            compoundCommitFileCapture.accept(compoundCommitFile, runnable);
        }, 1)) {

            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(3);
            StatelessCommitRef firstCommit = commitRefs.get(0);
            StatelessCommitRef secondCommit = commitRefs.get(1);
            StatelessCommitRef thirdCommit = commitRefs.get(2);

            commitFileToBlock.set(
                firstCommit.getAdditionalFiles()
                    .stream()
                    .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                    .findFirst()
                    .get()
            );

            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();
            firstCommitFile.set(compoundCommitFiles.get(0));
            secondCommitFile.set(compoundCommitFiles.get(1));

            testHarness.commitService.onCommitCreation(firstCommit);
            // todo: reevaluate the need to upload every commit.
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, firstCommit.getGeneration());
            safeAwait(startingUpload);
            assertThat(uploadedBlobs, not(hasItems(commitFileToBlock.get())));
            assertThat(uploadedBlobs, not(hasItems(firstCommitFile.get())));

            testHarness.commitService.onCommitCreation(secondCommit);
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, secondCommit.getGeneration());

            assertThat(uploadedBlobs, not(hasItems(secondCommitFile.get())));

            PlainActionFuture<Void> listener = new PlainActionFuture<>();
            ActionListener<Void> handoffListener = testHarness.commitService.markRelocating(testHarness.shardId, 1, listener);

            testHarness.commitService.onCommitCreation(thirdCommit);
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, thirdCommit.getGeneration());

            assertFalse(listener.isDone());

            blocking.countDown();

            listener.actionGet();

            handoffListener.onFailure(new IllegalStateException("Failed"));
            PlainActionFuture<Void> allowedListener = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, thirdCommit.getGeneration(), allowedListener);
            allowedListener.actionGet();

            ArrayList<String> uploadedFiles = new ArrayList<>(uploadedBlobs);

            assertThat(
                "Expected that compound commit file " + compoundCommitFiles.get(0) + " had been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItem(compoundCommitFiles.get(0))
            );

            assertThat(
                "Expected that compound commit file " + compoundCommitFiles.get(1) + " had been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItem(compoundCommitFiles.get(1))
            );

            assertThat(
                "Expected that compound commit file " + compoundCommitFiles.get(2) + " had been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItem(compoundCommitFiles.get(2))
            );
        }
    }

    public void testMapIsPrunedOnIndexDelete() throws Exception {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run(), 1)) {
            List<StatelessCommitRef> refs = testHarness.generateIndexCommits(2, true);
            StatelessCommitRef firstCommit = refs.get(0);
            StatelessCommitRef secondCommit = refs.get(1);

            testHarness.commitService.onCommitCreation(firstCommit);
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, firstCommit.getGeneration());

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future);
            future.actionGet();

            testHarness.commitService.onCommitCreation(secondCommit);
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, secondCommit.getGeneration());

            PlainActionFuture<Void> future2 = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future2);
            future2.actionGet();

            Set<String> files = testHarness.commitService.getFilesWithBlobLocations(testHarness.shardId);

            assertThat(
                "Expected that all first commit files " + firstCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(firstCommit.getCommitFiles().toArray(String[]::new))
            );
            assertThat(
                "Expected that all second commit files " + secondCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(secondCommit.getCommitFiles().toArray(String[]::new))
            );

            testHarness.commitService.markCommitDeleted(testHarness.shardId, firstCommit.getGeneration());
            var indexEngineLocalReaderListenerForShard = testHarness.commitService.getIndexEngineLocalReaderListenerForShard(
                testHarness.shardId
            );
            indexEngineLocalReaderListenerForShard.onLocalReaderClosed(
                firstCommit.getGeneration(),
                Set.of(getPrimaryTermAndGenerationForCommit(secondCommit))
            );

            List<String> expectedDeletedFiles = firstCommit.getCommitFiles()
                .stream()
                .filter(f -> secondCommit.getCommitFiles().contains(f) == false)
                .toList();

            // It might take a while until the reference held to send the new commit notification is released
            assertBusy(() -> {
                Set<String> filesAfterDelete = testHarness.commitService.getFilesWithBlobLocations(testHarness.shardId);

                assertThat(
                    "Expected that all first commit only files "
                        + expectedDeletedFiles
                        + " to be deleted from file map "
                        + filesAfterDelete,
                    filesAfterDelete,
                    not(hasItems(expectedDeletedFiles.toArray(String[]::new)))
                );
                assertThat(
                    "Expected that all second commit files " + secondCommit.getCommitFiles() + " to be in file map " + files,
                    files,
                    hasItems(secondCommit.getCommitFiles().toArray(String[]::new))
                );
            });
        }
    }

    public void testRecoveredCommitIsNotUploadedAgain() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs), 1)) {

            StatelessCommitRef commitRef = testHarness.generateIndexCommits(1).get(0);

            List<String> commitFiles = commitRef.getCommitFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            testHarness.commitService.onCommitCreation(commitRef);
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, commitRef.getGeneration());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
            future.actionGet();

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, List.of(blobNameFromGeneration(commitRef.getGeneration()))));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that the compound commit file "
                    + blobNameFromGeneration(commitRef.getGeneration())
                    + " has been uploaded "
                    + uploadedFiles,
                uploadedFiles,
                hasItems(blobNameFromGeneration(commitRef.getGeneration()))
            );

            uploadedBlobs.clear();

            var indexingShardState = readIndexingShardState(testHarness, primaryTerm);

            testHarness.commitService.closeShard(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId, primaryTerm, () -> false, (checkpoint, gcpListener, timeout) -> {
                gcpListener.accept(Long.MAX_VALUE, null);
            }, () -> {});
            testHarness.commitService.markRecoveredBcc(
                testHarness.shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs()
            );

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future2 = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future2);
            future2.actionGet();

            assertThat(uploadedBlobs, empty());
        }
    }

    public void testWaitForGenerationFailsForClosedShard() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run(), 1)) {
            ShardId unregisteredShardId = new ShardId("index", "uuid", 0);
            expectThrows(
                AlreadyClosedException.class,
                () -> testHarness.commitService.addListenerForUploadedGeneration(unregisteredShardId, 1, new PlainActionFuture<Void>())
            );
            PlainActionFuture<Void> failedFuture = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, 1, failedFuture);

            testHarness.commitService.closeShard(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, failedFuture::actionGet);
        }
    }

    public void testCommitsTrackingTakesIntoAccountSearchNodeUsage() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            long lastInitialGeneration = initialCommits.get(initialCommits.size() - 1).getGeneration();
            commitService.ensureMaxGenerationToUploadForFlush(shardId, lastInitialGeneration);
            waitUntilBCCIsUploaded(commitService, shardId, lastInitialGeneration);
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                new PrimaryTermAndGeneration(primaryTerm, lastInitialGeneration),
                new PrimaryTermAndGeneration(primaryTerm, lastInitialGeneration)
            );

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            markDeletedAndLocalUnused(List.of(mergedCommit), initialCommits, commitService, shardId);

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            PrimaryTermAndGeneration mergePTG = new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration());
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(mergePTG, mergePTG);

            var expectedDeletedCommits = uploadedCommits.stream()
                .filter(ptg -> mergePTG.equals(ptg) == false)
                .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOldCommitsAreRetainedIfSearchNodesUseThem() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            var firstCommit = initialCommits.get(0);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
                fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, firstCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            markDeletedAndLocalUnused(List.of(mergedCommit), initialCommits, commitService, shardId);

            // Commits 0-9 are reported to be used by the search node;
            // therefore we should retain them even if the indexing node has deleted them
            assertThat(deletedCommits, empty());

            // Now the search shard, report that it uses commit 0 and commit 10; therefore we should delete commits [1, 8]
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration()),
                new PrimaryTermAndGeneration(primaryTerm, firstCommit.getGeneration()),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = initialCommits.stream()
                .map(commit -> staleCommit(shardId, commit))
                .filter(commit -> commit.primaryTermAndGeneration().generation() != firstCommit.getGeneration())
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOldCommitsAreRetainedIfIndexShardUseThem() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            var firstCommit = initialCommits.get(0);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
            }

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);

            for (StatelessCommitRef indexCommit : initialCommits) {
                commitService.markCommitDeleted(shardId, indexCommit.getGeneration());
            }

            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            // Commits 0-9 are still used by the indexing shard
            // therefore we should retain them even if the indexing node/lucene has deleted them
            assertThat(deletedCommits, empty());

            // Now the index shard, report that it no longer uses commits [1;9] and they should be deleted
            commitService.getIndexEngineLocalReaderListenerForShard(shardId)
                .onLocalReaderClosed(
                    initialCommits.get(initialCommits.size() - 1).getGeneration(),
                    Set.of(getPrimaryTermAndGenerationForCommit(firstCommit), getPrimaryTermAndGenerationForCommit(mergedCommit))
                );

            var expectedDeletedCommits = initialCommits.stream()
                .map(commit -> staleCommit(shardId, commit))
                .filter(commit -> commit.primaryTermAndGeneration().generation() != firstCommit.getGeneration())
                .collect(Collectors.toSet());
            // the external reader notification and thus delete can go on after the upload completed.
            assertBusy(() -> assertThat(deletedCommits, equalTo(expectedDeletedCommits)));
        }
    }

    public void testRetainedCommitsAreReleasedAfterANodeIsUnassigned() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }

            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueMillis(20))
                    .put(SHARD_INACTIVITY_DURATION_TIME_SETTING.getKey(), TimeValue.timeValueMillis(1))
                    .build();
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);
            String searchNodeId = state.routingTable().shardRoutingTable(shardId).assignedUnpromotableShards().get(0).currentNodeId();
            fakeSearchNode.setSearchDiscoveryNode(state.getNodes().get(searchNodeId));

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }
            PrimaryTermAndGeneration lastInitialCommit = new PrimaryTermAndGeneration(
                primaryTerm,
                initialCommits.get(initialCommits.size() - 1).getGeneration()
            );
            commitService.ensureMaxGenerationToUploadForFlush(shardId, lastInitialCommit.generation());
            waitUntilBCCIsUploaded(commitService, shardId, lastInitialCommit.generation());

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            PrimaryTermAndGeneration mergePTG = new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration());
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            markDeletedAndLocalUnused(List.of(mergedCommit), initialCommits, commitService, shardId);

            assertThat(deletedCommits, empty());

            // The search node is still using commit 9 and the merged commit; therefore we should keep all commits around
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(mergePTG, lastInitialCommit, mergePTG);

            assertThat(deletedCommits, empty());

            var unassignedSearchShard = shardRoutingBuilder(shardId, null, false, ShardRoutingState.UNASSIGNED).withRole(
                ShardRouting.Role.SEARCH_ONLY
            ).build();

            var clusterStateWithUnassignedSearchShard = ClusterState.builder(state)
                .routingTable(
                    RoutingTable.builder()
                        .add(
                            IndexRoutingTable.builder(shardId.getIndex())
                                .addShard(state.routingTable().shardRoutingTable(shardId).primaryShard())
                                .addShard(unassignedSearchShard)
                        )
                        .build()
                )
                .build();

            // Once the search shard is removed from the search node below, the search node should return no in-use commits for any
            // NewCommitNotification request's generation.
            fakeSearchNode.doNotReturnCommitsOnNewNotification(true);

            logger.info("Releasing old commits, current commit generation is " + mergedCommit.getGeneration());
            commitService.clusterChanged(new ClusterChangedEvent("unassigned search shard", clusterStateWithUnassignedSearchShard, state));

            // The ShardInactivityMonitor will take a moment to run after the clusterChanged event, per the
            // SHARD_INACTIVITY_MONITOR_INTERVAL_TIME_SETTING and SHARD_INACTIVITY_DURATION_TIME_SETTING settings.
            assertBusy(() -> {
                var expectedDeletedCommits = uploadedCommits.stream()
                    .filter(ptg -> mergePTG.equals(ptg) == false)
                    .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                    .collect(Collectors.toSet());
                assertThat(deletedCommits, equalTo(expectedDeletedCommits));
            });
        }
    }

    public void testOutOfOrderNewCommitNotifications() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = Collections.synchronizedSet(new HashSet<>(2));
        var deletedCommitsLatch = new CountDownLatch(2);

        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
                deletedCommitsLatch.countDown();
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(2);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
            }

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            markDeletedAndLocalUnused(List.of(mergedCommit), initialCommits, commitService, shardId);

            assertThat(deletedCommits, empty());

            // The search shard is using the latest commit, but there are concurrent in-flight notifications
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration()),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            if (randomBoolean()) {
                commitService.markCommitDeleted(shardId, mergedCommit.getGeneration());
            }

            for (StatelessCommitRef initialCommit : initialCommits) {
                fakeSearchNode.respondWithUsedCommits(
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            // There were multiple in-flight new commit notification responses, but the notification filter by generation should ensure
            // we still delete all initial files.
            safeAwait(deletedCommitsLatch);
            assertThat(deletedCommits, equalTo(staleCommits(initialCommits, shardId)));
        }
    }

    public void testCommitsAreReleasedImmediatelyAfterDeletionWhenThereAreZeroReplicas() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NoOpNodeClient(threadPool) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(new NewCommitNotificationResponse(Set.of()));
                    }
                };
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }

            @Override
            protected long getPrimaryTerm() {
                return primaryTerm;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(2);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommits.get(initialCommits.size() - 1).getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, initialCommits.get(initialCommits.size() - 1).getGeneration());

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            PrimaryTermAndGeneration mergePTG = new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration());
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            markDeletedAndLocalUnused(List.of(mergedCommit), initialCommits, commitService, shardId);

            var expectedDeletedCommits = uploadedCommits.stream()
                .filter(ptg -> mergePTG.equals(ptg) == false)
                .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                .collect(Collectors.toSet());
            assertBusy(() -> assertThat(deletedCommits, is(equalTo(expectedDeletedCommits))));
        }
    }

    public void testCommitsAreSuccessfullyInitializedAfterRecovery() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            StatelessCommitRef recoveryCommit = initialCommits.get(initialCommits.size() - 1);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, recoveryCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, recoveryCommit.getGeneration());

            var indexingShardState = readIndexingShardState(testHarness, primaryTerm);

            testHarness.commitService.closeShard(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId, primaryTerm, () -> false, (checkpoint, gcpListener, timeout) -> {
                gcpListener.accept(Long.MAX_VALUE, null);
            }, () -> {});
            testHarness.commitService.markRecoveredBcc(
                testHarness.shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs()
            );

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            PrimaryTermAndGeneration mergePTG = new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration());
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            assert recoveryCommit.getGeneration() == indexingShardState.latestCommit().lastCompoundCommit().generation();
            commitService.markCommitDeleted(shardId, recoveryCommit.getGeneration());
            commitService.getIndexEngineLocalReaderListenerForShard(shardId)
                .onLocalReaderClosed(recoveryCommit.getGeneration(), Set.of(getPrimaryTermAndGenerationForCommit(mergedCommit)));

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration()),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = uploadedCommits.stream()
                .filter(ptg -> mergePTG.equals(ptg) == false)
                .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                .collect(Collectors.toSet());
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    /**
     * This test verifies only that the operation can complete successfully.
     *
     * For 50K, it runs in &lt; 1min and successfully at 100MB heap. At 100K, it also succeeds, but takes 4 mins.
     */
    public void testLargeRecovery() throws IOException {
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm)) {
            var shardId = testHarness.shardId;
            StatelessCompoundCommit recoveredCommit = new StatelessCompoundCommit(
                shardId,
                new PrimaryTermAndGeneration(2L, 1L),
                1L,
                "xx",
                Map.of(
                    "segments_2",
                    new BlobLocation(
                        new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(1), new PrimaryTermAndGeneration(2, 1)),
                        12,
                        12
                    )
                ),
                10,
                Set.of("segments_2"),
                0L,
                InternalFilesReplicatedRanges.EMPTY
            );
            int count = rarely() ? 50000 : 10000;
            var unreferencedFiles = IntStream.range(1, count)
                .mapToObj(i -> new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(i), new PrimaryTermAndGeneration(1, i)))
                .collect(Collectors.toSet());
            testHarness.commitService.markRecoveredBcc(
                testHarness.shardId,
                new BatchedCompoundCommit(recoveredCommit.primaryTermAndGeneration(), List.of(recoveredCommit)),
                unreferencedFiles
            );
        }
    }

    public void testUnreferencedCommitsAreReleasedAfterRecovery() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommit.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, initialCommit.getGeneration());
                fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                new PrimaryTermAndGeneration(primaryTerm, mergedCommit.getGeneration()),
                new PrimaryTermAndGeneration(primaryTerm, mergedCommit.getGeneration())
            );

            testHarness.commitService.closeShard(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);

            var indexingShardState = readIndexingShardState(testHarness, primaryTerm);

            testHarness.commitService.register(testHarness.shardId, primaryTerm, () -> false, (checkpoint, gcpListener, timeout) -> {
                gcpListener.accept(Long.MAX_VALUE, null);
            }, () -> {});
            testHarness.commitService.markRecoveredBcc(
                testHarness.shardId,
                indexingShardState.latestCommit(),
                indexingShardState.otherBlobs()
            );

            StatelessCommitRef recoveryCommit = mergedCommit;
            assert recoveryCommit.getGeneration() == indexingShardState.latestCommit().lastCompoundCommit().generation();

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            StatelessCommitRef newCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            StatelessCommitRef retainedBySearch = initialCommits.get(1);
            commitService.onCommitCreation(newCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, newCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, newCommit.getGeneration());
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                new PrimaryTermAndGeneration(primaryTerm, newCommit.getGeneration()),
                new PrimaryTermAndGeneration(primaryTerm, newCommit.getGeneration()),
                new PrimaryTermAndGeneration(primaryTerm, retainedBySearch.getGeneration())
            );

            // Retaining the second commit by the search node will retain the second AND first commit
            var expectedDeletedCommits = initialCommits.stream()
                .filter(
                    ref -> ref.getGeneration() != retainedBySearch.getGeneration()
                        && ref.getGeneration() != retainedBySearch.getGeneration() - 1
                )
                .map(commit -> staleCommit(shardId, commit))
                .collect(Collectors.toSet());
            // The uploaded commit notification is sent via
            // StatelessCommitNotificationPublisher#sendNewUploadedCommitNotificationAndFetchInUseCommits which has two parts
            // as the method name indicates (1) sending the notification and (2) fetching in-use commits.
            // Though the above respondWithUsedCommitsToUploadNotify call completes the listener for uploaded commit notification
            // Synchronously, the fetching in-use commits listener may have not been completed by then. In this case, the overall
            // process for processing CommitsInUse becomes Async which means the commits deletion may _not_ have happened
            // when respondWithUsedCommitsToUploadNotify returns. Hence, we need wrap the below assertion with assertBusy.
            assertBusy(() -> assertThat(deletedCommits, equalTo(expectedDeletedCommits)));

            commitService.markCommitDeleted(shardId, recoveryCommit.getGeneration());
            commitService.getIndexEngineLocalReaderListenerForShard(shardId)
                .onLocalReaderClosed(recoveryCommit.getGeneration(), Set.of(getPrimaryTermAndGenerationForCommit(newCommit)));
            HashSet<StaleCompoundCommit> newDeleted = new HashSet<>(expectedDeletedCommits);
            newDeleted.add(staleCommit(shardId, recoveryCommit));
            assertThat(deletedCommits, equalTo(newDeleted));
        }
    }

    public void testRegisterUnpromotableRecovery() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        AtomicBoolean activateSearchNode = new AtomicBoolean();
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NoOpNodeClient(threadPool) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        assert action == TransportNewCommitNotificationAction.TYPE;
                        if (activateSearchNode.get()) {
                            fakeSearchNode.doExecute(action, request, listener);
                        } else {
                            ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(
                                new NewCommitNotificationResponse(Set.of())
                            );
                        }
                    }
                };
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            ClusterState stateWithNoSearchShards = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            stateRef.set(stateWithNoSearchShards);
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );

            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var commit = initialCommits.get(initialCommits.size() - 1);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);
            activateSearchNode.set(true);

            boolean clusterChangedFirst = randomBoolean();
            if (clusterChangedFirst) {
                commitService.clusterChanged(new ClusterChangedEvent("test", state, stateWithNoSearchShards));
            }
            PlainActionFuture<RegisterCommitResponse> registerFuture = new PlainActionFuture<>();
            commitService.registerCommitForUnpromotableRecovery(
                null,
                new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration()),
                shardId,
                state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0).currentNodeId(),
                state,
                registerFuture
            );
            if (clusterChangedFirst == false) {
                assertThat(registerFuture.isDone(), is(false));
                commitService.clusterChanged(new ClusterChangedEvent("test", state, stateWithNoSearchShards));
            }
            PrimaryTermAndGeneration registeredCommit = registerFuture.actionGet().getCompoundCommit().primaryTermAndGeneration();
            assertThat(registeredCommit, equalTo(new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration())));

            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            markDeletedAndLocalUnused(List.of(mergedCommit), initialCommits, commitService, shardId);

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            PrimaryTermAndGeneration mergePTG = new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration());
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(mergePTG, mergePTG);

            var expectedDeletedCommits = uploadedCommits.stream()
                .filter(ptg -> mergePTG.equals(ptg) == false)
                .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                .collect(Collectors.toSet());
            assertBusy(
                () -> assertThat(
                    Sets.difference(deletedCommits, expectedDeletedCommits).toString(),
                    deletedCommits,
                    equalTo(expectedDeletedCommits)
                )
            );
        }
    }

    public void testAllCommitsAreDeletedWhenIndexIsDeleted() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            testHarness.commitService.addConsumerForNewUploadedBcc(
                testHarness.shardId,
                uploadedBccInfo -> uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration())
            );

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);

            int numberOfCommits = randomIntBetween(1, 10);
            var initialCommits = testHarness.generateIndexCommits(numberOfCommits);
            var delayedNewCommitNotifications = randomSubsetOf(initialCommits);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                if (delayedNewCommitNotifications.contains(initialCommit) == false) {
                    PrimaryTermAndGeneration primaryTermAndGeneration = new PrimaryTermAndGeneration(
                        primaryTerm,
                        initialCommit.getGeneration()
                    );
                    fakeSearchNode.respondWithUsedCommits(primaryTermAndGeneration, primaryTermAndGeneration);
                }
            }

            commitService.ensureMaxGenerationToUploadForFlush(shardId, initialCommits.get(initialCommits.size() - 1).getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, initialCommits.get(initialCommits.size() - 1).getGeneration());

            // Wait for sending all new commit notification requests, to be able to respond to them.
            assertBusy(() -> {
                int notifications = 0;
                for (var notificationFuture : fakeSearchNode.generationPendingListeners.values()) {
                    // also wait for "new uploaded commit notification" request to be processed so that the search node is registered
                    // for all commits (in BlobReference.searchNodesRef) and the following deletion and unregister will be able to
                    // fully decref the BlobReference.
                    assertThat(notificationFuture.upload.isDone(), equalTo(true));
                    notifications++;
                }
                assertThat(notifications, equalTo(numberOfCommits));
            });

            logger.info("--> Deleting shard {}", shardId);
            testHarness.commitService.delete(testHarness.shardId);
            testHarness.commitService.closeShard(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);

            for (StatelessCommitRef initialCommit : delayedNewCommitNotifications) {
                PrimaryTermAndGeneration primaryTermAndGeneration = new PrimaryTermAndGeneration(
                    primaryTerm,
                    initialCommit.getGeneration()
                );
                logger.info("--> Responding to new commit notification {}", primaryTermAndGeneration);
                fakeSearchNode.respondWithUsedCommits(primaryTermAndGeneration, primaryTermAndGeneration);
            }

            // All commits must be deleted
            var expectedDeletedCommits = uploadedCommits.stream()
                .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                .collect(Collectors.toSet());
            assertBusy(() -> assertThat(deletedCommits, is(expectedDeletedCommits)));
        }
    }

    /**
     * This test runs two concurrent threads to run indexing and search shard recoveries simultaneously. Cluster state changes are also
     * made to simulate real shard recoveries, adding and removing the search node from the cluster state around each shard recovery.
     *
     * The search node is mocked via canned TransportAction responses. Some coordination between threads is necessary so that the index
     * thread can generate new commits and the search thread can respond correctly to each {@link TransportNewCommitNotificationAction}.
     */
    @TestIssueLogging(
        value = "co.elastic.elasticsearch.stateless.commits.StatelessCommitService:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch-serverless/issues/2175"
    )
    public void testConcurrentIndexingAndSearchShardRecoveries() throws Exception {
        Set<PrimaryTermAndGeneration> uploadedCommits = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                logger.info("deleting vbcc {}", staleCompoundCommit);
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                return Optional.of(stateRef.get().routingTable().shardRoutingTable(shardId));
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return commitCleaner;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            testHarness.commitService.addConsumerForNewUploadedBcc(testHarness.shardId, uploadedBccInfo -> {
                logger.info("uploading vbcc {}", uploadedBccInfo.uploadedBcc().primaryTermAndGeneration());
                uploadedCommits.add(uploadedBccInfo.uploadedBcc().primaryTermAndGeneration());
            });

            var noSearchState = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            var searchState = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(noSearchState);
            String searchNodeId = searchState.routingTable().shardRoutingTable(shardId).assignedUnpromotableShards().get(0).currentNodeId();
            fakeSearchNode.setSearchDiscoveryNode(searchState.getNodes().get(searchNodeId));

            CyclicBarrier startBarrier = new CyclicBarrier(3);
            AtomicBoolean runIndexing = new AtomicBoolean(true);
            AtomicLong indexingRoundsCompleted = new AtomicLong();

            List<StatelessCommitRef> initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(1, 5));
            initialCommits.forEach(commitService::onCommitCreation);
            Set<PrimaryTermAndGeneration> allCommits = ConcurrentCollections.newConcurrentSet();
            initialCommits.stream()
                .map(commit -> new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration()))
                .forEach(allCommits::add);

            StatelessCommitRef initialLatestCommit = initialCommits.get(initialCommits.size() - 1);
            AtomicReference<PrimaryTermAndGeneration> latestCommit = new AtomicReference<>(
                new PrimaryTermAndGeneration(initialLatestCommit.getPrimaryTerm(), initialLatestCommit.getGeneration())
            );
            AtomicReference<PrimaryTermAndGeneration> latestUpload = new AtomicReference<>(null);

            // Flags to coordinate the cluster state change events between the two threads. Since each transport action is mocked, we need
            // to handle the special case of removing a node and provoking an uncommon TransportFetchShardCommitsInUseAction.
            AtomicBoolean pauseIndexing = new AtomicBoolean(false);
            AtomicBoolean indexingPaused = new AtomicBoolean(false);
            AtomicBoolean askIndexThreadToRunCommit = new AtomicBoolean(false);

            Thread indexer = new Thread(new Runnable() {
                List<StatelessCommitRef> previous = initialCommits;

                /**
                 * Creates a random number of commits in the {@link StatelessCommitService}. If runFinalMerge is set, pivots to creating a
                 * single merge commit to allow all the older commits to be deleted.
                 *
                 * @param runFinalMerge Controls whether to run a single merge commit.
                 */
                private void runCommits(boolean runFinalMerge) throws IOException {
                    List<StatelessCommitRef> newCommits = new ArrayList<>();
                    if (runFinalMerge) {
                        newCommits.add(testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0));
                    } else {
                        newCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(between(1, 5));
                    }
                    newCommits.forEach(commitService::onCommitCreation);
                    newCommits.stream()
                        .map(commit -> new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration()))
                        .forEach(allCommits::add);

                    long generation = newCommits.get(newCommits.size() - 1).getGeneration();
                    if (randomBoolean() || runFinalMerge) {
                        StatelessCommitRef randomCommit = randomFrom(newCommits);
                        latestCommit.set(new PrimaryTermAndGeneration(primaryTerm, randomCommit.getGeneration()));
                    }
                    commitService.ensureMaxGenerationToUploadForFlush(shardId, generation);
                    waitUntilBCCIsUploaded(commitService, shardId, generation);

                    if (runFinalMerge) {
                        logger.info("responding with final commit used by search nodes {}", latestCommit.get());
                        // The search thread isn't running anymore, so reply here.
                        fakeSearchNode.respondWithUsedCommitsToUploadNotify(
                            new PrimaryTermAndGeneration(primaryTerm, generation),
                            latestCommit.get()
                        );
                    }

                    latestUpload.set(new PrimaryTermAndGeneration(primaryTerm, generation));
                    markDeletedAndLocalUnused(newCommits, previous, commitService, shardId);
                    previous = newCommits;
                    Thread.yield();
                }

                @Override
                public void run() {
                    safeAwait(startBarrier);
                    try {
                        while (runIndexing.get()) {
                            runCommits(false);
                            indexingRoundsCompleted.incrementAndGet();
                        }

                        // Finish by running a merge commit through the index and search nodes, to delete all the older commits.
                        runCommits(true);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, "indexer");

            Thread searcher = new Thread(new Runnable() {
                long generationResponded = latestCommit.get().generation();

                public void respond(long toGeneration, PrimaryTermAndGeneration... commits) {
                    respond(toGeneration, Set.of(commits));
                }

                public void respond(long toGeneration, Set<PrimaryTermAndGeneration> commits) {
                    while (generationResponded < toGeneration) {
                        ++generationResponded;
                        // we generate commits with holes in the sequence, skip those.
                        PrimaryTermAndGeneration primaryTermAndGeneration = new PrimaryTermAndGeneration(primaryTerm, generationResponded);
                        if (allCommits.contains(primaryTermAndGeneration)) {
                            fakeSearchNode.respondWithUsedCommits(primaryTermAndGeneration, commits);
                        }
                    }
                    PrimaryTermAndGeneration uploaded = latestUpload.get();
                    if (uploaded != null) {
                        fakeSearchNode.respondWithUsedCommitsToUploadNotify(uploaded, commits.toArray(new PrimaryTermAndGeneration[0]));
                    }
                }

                @Override
                public void run() {
                    safeAwait(startBarrier);
                    for (long i = 0; i < 10 || indexingRoundsCompleted.get() < 10; ++i) {
                        respond(latestCommit.get().primaryTerm());
                        var clusterState = changeClusterState(searchState);

                        PlainActionFuture<RegisterCommitResponse> future = new PlainActionFuture<>();

                        var latestUploadedBcc = commitService.getLatestUploadedBcc(shardId);
                        commitService.registerCommitForUnpromotableRecovery(
                            latestUploadedBcc != null ? latestUploadedBcc.primaryTermAndGeneration() : ZERO,
                            latestUploadedBcc != null ? latestUploadedBcc.lastCompoundCommit().primaryTermAndGeneration() : ZERO,
                            shardId,
                            searchState.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0).currentNodeId(),
                            clusterState,
                            future
                        );
                        PrimaryTermAndGeneration searchRecoveredGeneration;
                        Set<PrimaryTermAndGeneration> usedCommits;
                        try {
                            var recoveryCommit = future.actionGet().getCompoundCommit();
                            searchRecoveredGeneration = recoveryCommit.primaryTermAndGeneration();
                            usedCommits = BatchedCompoundCommit.computeReferencedBCCGenerations(recoveryCommit);
                        } catch (NoShardAvailableActionException e) {
                            // todo: avoid the NoShardAvailableException when not warranted.
                            continue;
                        }

                        respond(searchRecoveredGeneration.generation(), usedCommits);
                        for (var usedCommit : usedCommits) {
                            assertThat(deletedCommits, not(hasItems(new StaleCompoundCommit(shardId, usedCommit, primaryTerm))));
                        }

                        changeClusterState(noSearchState);
                    }
                }

                private ClusterState changeClusterState(ClusterState state) {
                    ClusterState previous = stateRef.get();
                    ClusterState next = ClusterState.builder(state).version(previous.getVersion() + 1).build();
                    stateRef.set(next);
                    commitService.clusterChanged(new ClusterChangedEvent("test search thread", next, previous));
                    return next;
                }
            }, "searcher");

            indexer.start();
            searcher.start();
            safeAwait(startBarrier);

            // Wait for searcher thread to finish, then signal the indexer to stop.
            searcher.join();
            runIndexing.set(false);
            indexer.join();

            PrimaryTermAndGeneration finalPTG = new PrimaryTermAndGeneration(
                latestCommit.get().primaryTerm(),
                latestCommit.get().generation()
            );
            var expectedDeletedCommits = uploadedCommits.stream()
                .filter(pTG -> finalPTG.equals(pTG) == false)
                .map(p -> new StaleCompoundCommit(shardId, p, primaryTerm))
                .collect(Collectors.toSet());

            assertBusy(
                () -> assertThat(
                    Sets.difference(expectedDeletedCommits, deletedCommits).toString(),
                    deletedCommits,
                    equalTo(expectedDeletedCommits)
                )
            );
        }
    }

    public void testRegisterCommitForUnpromotableRecovery() throws Exception {
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm)) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            ClusterState stateWithNoSearchShards = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            ClusterState stateWithSearchShards = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            var initialCommits = testHarness.generateIndexCommits(10);
            var commit = initialCommits.get(initialCommits.size() - 1);
            var nodeId = stateWithSearchShards.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0).currentNodeId();

            commitService.clusterChanged(new ClusterChangedEvent("test", stateWithSearchShards, stateWithNoSearchShards));
            // Registration sent to an un-initialized shard
            var commitToRegister = new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration());
            PlainActionFuture<RegisterCommitResponse> registerFuture = new PlainActionFuture<>();
            commitService.registerCommitForUnpromotableRecovery(
                null,
                commitToRegister,
                shardId,
                nodeId,
                stateWithSearchShards,
                registerFuture
            );
            expectThrows(NoShardAvailableActionException.class, registerFuture::actionGet);
            // Registering after initialization is done should work
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }
            testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, commit.getGeneration());
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commit.getGeneration(), future);
            future.actionGet();
            registerFuture = new PlainActionFuture<>();
            commitService.registerCommitForUnpromotableRecovery(
                null,
                commitToRegister,
                shardId,
                nodeId,
                stateWithSearchShards,
                registerFuture
            );
            var registrationResponse = registerFuture.get();
            assertThat(registrationResponse, notNullValue());
            assertThat(registrationResponse.getCompoundCommit().primaryTermAndGeneration(), equalTo(commitToRegister));
        }
    }

    public void testRegisterCommitForUnpromotableRecoveryAcquiresARefForAllBlobs() throws Exception {
        Set<StaleCompoundCommit> deletedBCCs = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode(threadPool);
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return new StatelessCommitCleaner(null, null, null) {
                    @Override
                    void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                        deletedBCCs.add(staleCompoundCommit);
                    }
                };
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected Optional<IndexShardRoutingTable> getShardRoutingTable(ShardId shardId) {
                return Optional.ofNullable(stateRef.get()).map(s -> s.routingTable().shardRoutingTable(shardId));
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            ClusterState stateWithNoSearchShards = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            ClusterState stateWithSearchShards = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            var initialCommits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(10);
            var firstBCC = initialCommits.get(0);
            var commit = initialCommits.get(initialCommits.size() - 1);
            var nodeId = stateWithSearchShards.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0).currentNodeId();

            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }
            commitService.ensureMaxGenerationToUploadForFlush(shardId, commit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());

            var mergedCommit = testHarness.generateIndexCommits(1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commit.getGeneration());
            var mergedCommitPTG = new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration());

            commitService.clusterChanged(new ClusterChangedEvent("test", stateWithSearchShards, stateWithNoSearchShards));
            stateRef.set(stateWithSearchShards);

            var registerFuture = new PlainActionFuture<RegisterCommitResponse>();
            commitService.registerCommitForUnpromotableRecovery(
                null,
                mergedCommitPTG,
                shardId,
                nodeId,
                stateWithSearchShards,
                registerFuture
            );
            var registrationResponse = registerFuture.get();
            assertThat(registrationResponse, notNullValue());
            assertThat(registrationResponse.getCompoundCommit().primaryTermAndGeneration(), equalTo(mergedCommitPTG));

            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.markCommitDeleted(shardId, initialCommit.getGeneration());
            }
            commitService.getIndexEngineLocalReaderListenerForShard(shardId)
                .onLocalReaderClosed(commit.getGeneration(), Set.of(mergedCommitPTG));

            assertThat(deletedBCCs, is(empty()));

            var commitAfterMerge = testHarness.generateIndexCommits(1, false).get(0);
            commitService.onCommitCreation(commitAfterMerge);

            commitService.ensureMaxGenerationToUploadForFlush(shardId, commitAfterMerge.getGeneration());
            waitUntilBCCIsUploaded(commitService, shardId, commitAfterMerge.getGeneration());

            var commitAfterMergePTG = new PrimaryTermAndGeneration(commitAfterMerge.getPrimaryTerm(), commitAfterMerge.getGeneration());
            fakeSearchNode.getListenerForNewCommitUpload(commitAfterMergePTG)
                .get()
                .onResponse(new NewCommitNotificationResponse(Set.of(mergedCommitPTG, commitAfterMergePTG)));

            assertThat(
                deletedBCCs,
                is(
                    equalTo(
                        Set.of(
                            new StaleCompoundCommit(
                                shardId,
                                new PrimaryTermAndGeneration(firstBCC.getPrimaryTerm(), firstBCC.getGeneration()),
                                primaryTerm
                            )
                        )
                    )
                )
            );
        }
    }

    public void testScheduledCommitUpload() throws Exception {
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    // Short interval to test scheduled upload behaviour
                    .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueMillis(50))
                    .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueMillis(30))
                    // Disable upload triggered by number of commits and size
                    .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
                    .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
                    .build();
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NoOpNodeClient(threadPool) {
                    @SuppressWarnings("unchecked")
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        assert action == TransportNewCommitNotificationAction.TYPE;
                        ((ActionListener<NewCommitNotificationResponse>) listener).onResponse(new NewCommitNotificationResponse(Set.of()));
                    }
                };
            }
        }) {
            final List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(between(5, 8));
            for (var commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
                // randomly delay a bit to allow more interleaving between indexing and scheduled upload
                safeSleep(randomLongBetween(10, 30));
            }

            // Assert all commits are uploaded
            assertBusy(() -> {
                assertThat(testHarness.commitService.getCurrentVirtualBcc(testHarness.shardId), nullValue());
                assertThat(testHarness.commitService.hasPendingBccUploads(testHarness.shardId), is(false));
                final BlobContainer blobContainer = testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm);
                final List<BatchedCompoundCommit> allBatchedCompoundCommits = blobContainer.listBlobs(OperationPurpose.INDICES)
                    .values()
                    .stream()
                    .filter(blobMetadata -> StatelessCompoundCommit.startsWithBlobPrefix(blobMetadata.name()))
                    .map(blobMetadata -> {
                        try {
                            return BatchedCompoundCommit.readFromStore(
                                blobMetadata.name(),
                                blobMetadata.length(),
                                (blobName, offset, length) -> new InputStreamStreamInput(
                                    blobContainer.readBlob(OperationPurpose.INDICES, blobName, offset, length)
                                ),
                                true
                            );
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .toList();

                assertThat(allBatchedCompoundCommits, not(empty()));
                final List<Long> expectedCcGenerations = commitRefs.stream().map(StatelessCommitRef::getGeneration).toList();
                assertThat(
                    allBatchedCompoundCommits.stream()
                        .flatMap(bcc -> bcc.compoundCommits().stream().map(StatelessCompoundCommit::generation))
                        .toList(),
                    equalTo(expectedCcGenerations)
                );
            });
        }
    }

    public void testShouldUploadVirtualBccWhenDelayedUploadAreEnabledAndMaxCommitsExceeded() throws Exception {
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    // todo: reevaluate this
                    .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                    .build();
            }
        }) {
            for (StatelessCommitRef commitRef : testHarness.generateIndexCommits(randomIntBetween(1, 4))) {
                testHarness.commitService.onCommitCreation(commitRef);
                assertThat(testHarness.commitService.getCurrentVirtualBcc(testHarness.shardId), nullValue());
                assertBusy(
                    () -> assertTrue(
                        testHarness.getShardContainer()
                            .blobExists(OperationPurpose.INDICES, StatelessCompoundCommit.blobNameFromGeneration(commitRef.getGeneration()))
                    )
                );
            }
        }
    }

    public void testShouldOnlyUploadVirtualBccWhenOverMaxCommitLimit() throws Exception {
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder().put(super.nodeSettings()).put("stateless.upload.max_commits", 2).build();
            }
        }) {
            List<StatelessCommitRef> commitRefs = testHarness.generateIndexCommits(2);
            // First commit
            testHarness.commitService.onCommitCreation(commitRefs.get(0));
            final var currentVirtualBcc = testHarness.commitService.getCurrentVirtualBcc(testHarness.shardId);
            assertThat(currentVirtualBcc, notNullValue());
            assertThat(currentVirtualBcc.isFrozen(), is(false));

            // Second commit
            testHarness.commitService.onCommitCreation(commitRefs.get(1));
            assertThat(testHarness.commitService.getCurrentVirtualBcc(testHarness.shardId), nullValue());
            getAndAssertBccWithGenerations(
                testHarness.getShardContainer(),
                commitRefs.get(0).getGeneration(),
                commitRefs.stream().map(StatelessCommitRef::getGeneration).toList()
            );
        }
    }

    public void testShouldOnlyUploadVirtualBccWhenOverMaxSizeLimit() throws Exception {
        long uploadMaxSize = randomLongBetween(1, ByteSizeValue.ofKb(20).getBytes());
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put("stateless.upload.max_commits", 100)
                    .put("stateless.upload.max_size", uploadMaxSize + "b")
                    .build();
            }
        }) {
            final List<StatelessCommitRef> allCommitRefs = new ArrayList<>();
            do {
                StatelessCommitRef commitRef = testHarness.generateIndexCommits(1).get(0);
                allCommitRefs.add(commitRef);
                testHarness.commitService.onCommitCreation(commitRef);
            } while (testHarness.commitService.getCurrentVirtualBcc(testHarness.shardId) != null);

            final var batchedCompoundCommit = getAndAssertBccWithGenerations(
                testHarness.getShardContainer(),
                allCommitRefs.get(0).getGeneration(),
                allCommitRefs.stream().map(StatelessCommitRef::getGeneration).toList()
            );

            final long[] ccSizes = batchedCompoundCommit.compoundCommits()
                .stream()
                .mapToLong(StatelessCompoundCommit::sizeInBytes)
                .toArray();
            long sizeBeforeUpload = 0;
            for (int i = 0; i <= ccSizes.length - 2; i++) {
                if (i == ccSizes.length - 2) {
                    sizeBeforeUpload += ccSizes[i];
                } else {
                    sizeBeforeUpload += BlobCacheUtils.toPageAlignedSize(ccSizes[i]);
                }
            }
            assertThat(sizeBeforeUpload, lessThan(uploadMaxSize));

            long totalSize = 0;
            for (int i = 0; i < ccSizes.length - 1; i++) {
                totalSize += BlobCacheUtils.toPageAlignedSize(ccSizes[i]);
            }
            totalSize += ccSizes[ccSizes.length - 1];
            assertThat(totalSize, greaterThanOrEqualTo(uploadMaxSize));
        }
    }

    public void testMarkCommitDeletedWithMultipleCCsPerBCC() throws Exception {
        var fakeSearchNode = new FakeSearchNode(threadPool);
        Set<StaleCompoundCommit> deletedBCCs = ConcurrentCollections.newConcurrentSet();
        int commitsPerBCC = 2;
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return new StatelessCommitCleaner(null, null, null) {
                    @Override
                    void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                        deletedBCCs.add(staleCompoundCommit);
                    }
                };
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }

            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), commitsPerBCC)
                    .build();
            }

        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;
            List<StatelessCommitRef> nonMergedCommits = new ArrayList<>();
            List<PrimaryTermAndGeneration> bccsWithNonMergedCommits = new ArrayList<>();

            var numberOfBCCs = randomIntBetween(2, 3);
            for (int i = 0; i < numberOfBCCs; i++) {
                List<StatelessCommitRef> commits = testHarness.generateIndexCommitsWithoutMergeOrDeletion(commitsPerBCC);
                nonMergedCommits.addAll(commits);

                commitService.onCommitCreation(commits.get(0));
                final var currentVirtualBcc = testHarness.commitService.getCurrentVirtualBcc(shardId);
                assertThat(currentVirtualBcc, notNullValue());
                assertThat(currentVirtualBcc.isFrozen(), is(false));
                bccsWithNonMergedCommits.add(currentVirtualBcc.getPrimaryTermAndGeneration());

                commitService.onCommitCreation(commits.get(1));
                assertThat(commitService.getCurrentVirtualBcc(shardId), nullValue());

                waitUntilBCCIsUploaded(commitService, shardId, currentVirtualBcc.getPrimaryTermAndGeneration().generation());
            }

            // Force merge so we can delete all previous commits
            var mergedCommit = testHarness.generateIndexCommits(1, true, false, generation -> {}).get(0);
            commitService.onCommitCreation(mergedCommit);
            commitService.ensureMaxGenerationToUploadForFlush(shardId, mergedCommit.getGeneration());

            waitUntilBCCIsUploaded(commitService, shardId, mergedCommit.getGeneration());

            assertThat(deletedBCCs, is(empty()));

            for (StatelessCommitRef commit : nonMergedCommits) {
                commitService.markCommitDeleted(shardId, commit.getGeneration());
                if (randomBoolean()) {
                    commitService.markCommitDeleted(shardId, commit.getGeneration());
                }
            }

            assertThat(deletedBCCs, is(empty()));

            var indexEngineLocalReaderListenerForShard = commitService.getIndexEngineLocalReaderListenerForShard(shardId);
            PrimaryTermAndGeneration mergePrimaryTermAndGeneration = new PrimaryTermAndGeneration(
                primaryTerm,
                mergedCommit.getGeneration()
            );
            indexEngineLocalReaderListenerForShard.onLocalReaderClosed(
                bccsWithNonMergedCommits.get(bccsWithNonMergedCommits.size() - 1).generation(),
                Set.of(mergePrimaryTermAndGeneration)
            );
            fakeSearchNode.respondWithUsedCommitsToUploadNotify(mergePrimaryTermAndGeneration, mergePrimaryTermAndGeneration);

            // We need assertBusy as we do an incRef for the BlobReference before we start the BCC upload
            // (in StatelessCommitService#createAndRunCommitUpload) and decRef once the BCC is uploaded,
            // a new commit notification is sent and the listener finishes, even-though we wait until the
            // new commit notification is sent, there's still a slight chance of the upload decRef running
            // after we call commitService.unregister.
            var expectedDeletedBCCs = bccsWithNonMergedCommits.stream()
                .map(bccGeneration -> new StaleCompoundCommit(shardId, bccGeneration, primaryTerm))
                .collect(Collectors.toSet());
            assertBusy(() -> assertThat(deletedBCCs, is(equalTo(expectedDeletedBCCs))));
        }
    }

    public void testMarkCommitDeletedIsIdempotent() throws Exception {
        var fakeSearchNode = new FakeSearchNode(threadPool);
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected StatelessCommitCleaner createCommitCleaner(
                StatelessClusterConsistencyService consistencyService,
                ThreadPool threadPool,
                ObjectStoreService objectStoreService
            ) {
                return new StatelessCommitCleaner(null, null, null) {
                    @Override
                    void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                        deletedCommits.add(staleCompoundCommit);
                    }
                };
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return fakeSearchNode;
            }
        }) {
            var shardId = testHarness.shardId;
            var commitService = testHarness.commitService;

            var numberOfCommits = randomIntBetween(2, 4);
            List<StatelessCommitRef> commits = testHarness.generateIndexCommits(numberOfCommits, randomBoolean());

            for (StatelessCommitRef commitRef : commits) {
                commitService.onCommitCreation(commitRef);
                commitService.ensureMaxGenerationToUploadForFlush(shardId, commitRef.getGeneration());
                waitUntilBCCIsUploaded(commitService, shardId, commitRef.getGeneration());

                // Wait until the new commit notification is sent
                fakeSearchNode.getListenerForNewCommitUpload(
                    new PrimaryTermAndGeneration(commitRef.getPrimaryTerm(), commitRef.getGeneration())
                ).get();

                if (randomBoolean()) {
                    // Call at least 3 times, ensuring that we do not decRef more than once and delete the blob accidentally
                    var numberOfCalls = randomIntBetween(3, 5);
                    for (int i = 0; i < numberOfCalls; i++) {
                        commitService.markCommitDeleted(shardId, commitRef.getGeneration());
                    }
                }
            }

            assertThat(deletedCommits, is(empty()));

            commitService.delete(shardId);
            commitService.closeShard(testHarness.shardId);
            commitService.unregister(shardId);

            // We need assertBusy as we do an incRef for the BlobReference before we start the BCC upload
            // (in StatelessCommitService#createAndRunCommitUpload) and decRef once the BCC is uploaded,
            // a new commit notification is sent and the listener finishes, even-though we wait until the
            // new commit notification is sent, there's still a slight chance of the upload decRef running
            // after we call commitService.unregister.
            assertBusy(() -> assertThat(deletedCommits, is(equalTo(staleCommits(commits, shardId)))));
        }
    }

    public void testReadVirtualBatchedCompoundCommitChunkWillWorkForHugeVBCC() throws IOException {
        final VirtualBatchedCompoundCommit virtualBcc = mock(VirtualBatchedCompoundCommit.class);
        final long vbccSize = randomLongBetween(Long.MAX_VALUE / 2, Long.MAX_VALUE);
        when(virtualBcc.getTotalSizeInBytes()).thenReturn(vbccSize);
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected StatelessCommitService createCommitService() {
                return new StatelessCommitService(
                    nodeSettings,
                    clusterService,
                    objectStoreService,
                    indicesService,
                    () -> clusterService.localNode().getEphemeralId(),
                    this::getShardRoutingTable,
                    clusterService.threadPool(),
                    client,
                    new StatelessCommitCleaner(null, null, null),
                    sharedCacheService,
                    warmingService,
                    telemetryProvider
                ) {
                    @Override
                    protected ShardCommitState createShardCommitState(
                        ShardId shardId,
                        long primaryTerm,
                        BooleanSupplier inititalizingNoSearchSupplier,
                        TriConsumer<
                            Long,
                            GlobalCheckpointListeners.GlobalCheckpointListener,
                            TimeValue> addGlobalCheckpointListenerFunction,
                        Runnable triggerTranslogReplicator
                    ) {
                        return new ShardCommitState(
                            shardId,
                            primaryTerm,
                            inititalizingNoSearchSupplier,
                            addGlobalCheckpointListenerFunction,
                            triggerTranslogReplicator
                        ) {
                            @Override
                            public VirtualBatchedCompoundCommit getVirtualBatchedCompoundCommit(
                                PrimaryTermAndGeneration primaryTermAndGeneration
                            ) {
                                return virtualBcc;
                            }
                        };
                    }
                };
            }
        }) {
            final long primaryTerm = randomLongBetween(1, 42);
            final long generation = randomLongBetween(1, 9999);
            final long offset = randomLongBetween(0, Long.MAX_VALUE / 2);
            final int length = randomNonNegativeInt();
            final var request = new GetVirtualBatchedCompoundCommitChunkRequest(
                testHarness.shardId,
                primaryTerm,
                generation,
                offset,
                length,
                "_na_"
            );
            final StreamOutput output = mock(StreamOutput.class);
            testHarness.commitService.readVirtualBatchedCompoundCommitChunk(request, output);

            final long effectiveLength;
            final long availableVbccSize = vbccSize - offset;
            if (availableVbccSize > Integer.MAX_VALUE) {
                effectiveLength = length;
            } else {
                effectiveLength = length < availableVbccSize ? length : availableVbccSize;
            }
            verify(virtualBcc, times(1)).getBytesByRange(eq(offset), eq(effectiveLength), eq(output));
        }
    }

    private BatchedCompoundCommit getAndAssertBccWithGenerations(BlobContainer shardContainer, long bccGeneration, List<Long> ccGenerations)
        throws Exception {
        assert bccGeneration == ccGenerations.get(0);
        final AtomicReference<BatchedCompoundCommit> bccRef = new AtomicReference<>();
        assertBusy(() -> {
            final String expectedBccBlobName = blobNameFromGeneration(bccGeneration);
            final BlobMetadata blobMedata = shardContainer.listBlobs(OperationPurpose.INDICES)
                .values()
                .stream()
                .filter(blobMetadata -> blobMetadata.name().equals(expectedBccBlobName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("commit blob not found"));
            final var batchedCompoundCommit = BatchedCompoundCommit.readFromStore(
                expectedBccBlobName,
                blobMedata.length(),
                (blobName, offset, length) -> new InputStreamStreamInput(
                    shardContainer.readBlob(OperationPurpose.INDICES, blobName, offset, length)
                ),
                true
            );
            assertThat(
                batchedCompoundCommit.compoundCommits().stream().map(StatelessCompoundCommit::generation).toList(),
                equalTo(ccGenerations)
            );
            bccRef.set(batchedCompoundCommit);
        });
        return bccRef.get();
    }

    public void testDoesNotCloseCommitReferenceOnceAppended() throws IOException {
        final var expectedException = new AssertionError("failure after append");
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NodeClient(nodeSettings, threadPool) {
                    @Override
                    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                        ActionType<Response> action,
                        Request request,
                        ActionListener<Response> listener
                    ) {
                        throw expectedException;
                    }
                };
            }
        }) {
            final StatelessCommitRef commitRef = spy(testHarness.generateIndexCommits(1).get(0));
            assertThat(
                expectThrows(AssertionError.class, () -> testHarness.commitService.onCommitCreation(commitRef)),
                sameInstance(expectedException)
            );
            final var virtualBcc = testHarness.commitService.getCurrentVirtualBcc(testHarness.shardId);
            assertThat(virtualBcc.getLastPendingCompoundCommit().getCommitReference(), sameInstance(commitRef));
            verify(commitRef, never()).close();
        }
    }

    private Set<StaleCompoundCommit> staleCommits(List<StatelessCommitRef> commits, ShardId shardId) {
        return commits.stream().map(commit -> staleCommit(shardId, commit)).collect(Collectors.toSet());
    }

    private StaleCompoundCommit staleCommit(ShardId shardId, StatelessCommitRef commit) {
        return new StaleCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()), primaryTerm);
    }

    private static class FakeSearchNode extends NoOpNodeClient {

        record NotificationFuture(
            PlainActionFuture<ActionListener<NewCommitNotificationResponse>> notification,
            PlainActionFuture<ActionListener<NewCommitNotificationResponse>> upload
        ) {}

        private final Map<PrimaryTermAndGeneration, NotificationFuture> generationPendingListeners = ConcurrentCollections
            .newConcurrentMap();
        private final Map<PrimaryTermAndGeneration, StatelessCompoundCommit> notifiedCommits = ConcurrentCollections.newConcurrentMap();
        private DiscoveryNode searchDiscoveryNode = null;
        private boolean doNotReturnCommitsOnNewNotification = false;

        FakeSearchNode(ThreadPool threadPool) {
            super(threadPool);
        }

        /**
         * The {@link TransportFetchShardCommitsInUseAction} needs the {@link DiscoveryNode} to create a response instance.
         */
        private void setSearchDiscoveryNode(DiscoveryNode searchDiscoveryNode) {
            this.searchDiscoveryNode = searchDiscoveryNode;
        }

        /**
         * Ignores any {@link #generationPendingListeners} and instead returns an empty set of commits for any
         * TransportNewCommitNotificationAction.
         */
        private void doNotReturnCommitsOnNewNotification(boolean val) {
            this.doNotReturnCommitsOnNewNotification = val;
        }

        void respondWithUsedCommits(PrimaryTermAndGeneration primaryTermAndGeneration, PrimaryTermAndGeneration... usedCommits) {
            // Ensure that the fake search node has been notified with all the commits, so we're able to resolve their BCC dependencies
            // and use them to notify back the index node
            getListenerForNewCommitNotification(primaryTermAndGeneration).actionGet();
            for (PrimaryTermAndGeneration usedCommit : usedCommits) {
                getListenerForNewCommitNotification(usedCommit).actionGet();
            }
            Set<PrimaryTermAndGeneration> allOpenBCCs = Arrays.stream(usedCommits)
                .map(notifiedCommits::get)
                .flatMap(statelessCompoundCommit -> {
                    Objects.requireNonNull(statelessCompoundCommit);
                    return BatchedCompoundCommit.computeReferencedBCCGenerations(statelessCompoundCommit).stream();
                })
                .collect(Collectors.toSet());
            respondWithUsedCommits(primaryTermAndGeneration, allOpenBCCs);
        }

        void respondWithUsedCommits(PrimaryTermAndGeneration primaryTermAndGeneration, Set<PrimaryTermAndGeneration> usedCommits) {
            getListenerForNewCommitNotification(primaryTermAndGeneration).actionGet()
                .onResponse(new NewCommitNotificationResponse(usedCommits));
        }

        void respondWithUsedCommitsToUploadNotify(
            PrimaryTermAndGeneration primaryTermAndGeneration,
            PrimaryTermAndGeneration... usedCommits
        ) {
            // Ensure that the fake search node has been notified with all the commits, so we're able to resolve their BCC dependencies
            // and use them to notify back the index node
            getListenerForNewCommitNotification(primaryTermAndGeneration).actionGet();
            for (PrimaryTermAndGeneration usedCommit : usedCommits) {
                getListenerForNewCommitNotification(usedCommit).actionGet();
            }
            Set<PrimaryTermAndGeneration> allOpenBCCs = Arrays.stream(usedCommits)
                .map(notifiedCommits::get)
                .flatMap(statelessCompoundCommit -> {
                    Objects.requireNonNull(statelessCompoundCommit);
                    return BatchedCompoundCommit.computeReferencedBCCGenerations(statelessCompoundCommit).stream();
                })
                .collect(Collectors.toSet());
            getListenerForNewCommitUpload(primaryTermAndGeneration).actionGet().onResponse(new NewCommitNotificationResponse(allOpenBCCs));
        }

        private PlainActionFuture<ActionListener<NewCommitNotificationResponse>> getListenerForNewCommitNotification(
            PrimaryTermAndGeneration primaryTermAndGeneration
        ) {
            return getNotificationFuture(primaryTermAndGeneration).notification;
        }

        private PlainActionFuture<ActionListener<NewCommitNotificationResponse>> getListenerForNewCommitUpload(
            PrimaryTermAndGeneration primaryTermAndGeneration
        ) {
            return getNotificationFuture(primaryTermAndGeneration).upload;
        }

        private NotificationFuture getNotificationFuture(PrimaryTermAndGeneration primaryTermAndGeneration) {
            return generationPendingListeners.computeIfAbsent(
                primaryTermAndGeneration,
                unused -> new NotificationFuture(new PlainActionFuture<>(), new PlainActionFuture<>())
            );
        }

        synchronized void onNewNotification(NewCommitNotificationRequest request, ActionListener<NewCommitNotificationResponse> listener) {
            if (doNotReturnCommitsOnNewNotification) {
                listener.onResponse(new NewCommitNotificationResponse(Set.of()));
                return;
            }

            notifiedCommits.putIfAbsent(request.getCompoundCommit().primaryTermAndGeneration(), request.getCompoundCommit());
            getNotificationFuture(request.getCompoundCommit().primaryTermAndGeneration()).notification.onResponse(listener);

            if (request.isUploaded()) {
                PrimaryTermAndGeneration bccPTG = new PrimaryTermAndGeneration(
                    request.getCompoundCommit().primaryTerm(),
                    request.getBatchedCompoundCommitGeneration()
                );
                ActionListener<NewCommitNotificationResponse> runOnce = ActionListener.notifyOnce(listener);
                for (PrimaryTermAndGeneration commit : notifiedCommits.keySet()) {
                    if (commit.onOrBefore(request.getCompoundCommit().primaryTermAndGeneration()) && commit.onOrAfter(bccPTG)) {
                        // The commit notification listener is added for every commit uploaded. However, the actual notification is only
                        // sent once. So limit to notifying back only once.
                        getNotificationFuture(commit).upload.onResponse(runOnce);
                    }
                }
            }
        }

        /**
         * Handles canned (preset) responses to any {@link TransportNewCommitNotificationAction} and
         * {@link TransportFetchShardCommitsInUseAction} requests.
         */
        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assert action == TransportNewCommitNotificationAction.TYPE || action == TransportFetchShardCommitsInUseAction.TYPE
                : "Unexpected ActionType: " + action;
            if (request instanceof NewCommitNotificationRequest) {
                onNewNotification((NewCommitNotificationRequest) request, (ActionListener<NewCommitNotificationResponse>) listener);
            } else {
                assert request instanceof FetchShardCommitsInUseAction.Request : "Unexpected request type: " + request.getClass().getName();
                assert searchDiscoveryNode != null;

                // Always return an empty commits-in-use response.
                ((ActionListener<FetchShardCommitsInUseAction.Response>) listener).onResponse(
                    new FetchShardCommitsInUseAction.Response(
                        ClusterName.DEFAULT,
                        List.of(new FetchShardCommitsInUseAction.NodeResponse(searchDiscoveryNode, Set.of())),
                        List.of()
                    )
                );
            }

        }
    }

    private static void waitUntilBCCIsUploaded(StatelessCommitService commitService, ShardId shardId, long bccGeneration) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        commitService.addListenerForUploadedGeneration(shardId, bccGeneration, future);
        future.actionGet();
    }

    private ClusterState clusterStateWithPrimaryAndSearchShards(ShardId shardId, int searchNodes) {
        var indexNode = DiscoveryNodeUtils.create(
            "index_node",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE)
        );

        var primaryShard = newShardRouting(shardId, indexNode.getId(), true, ShardRoutingState.STARTED);

        var discoveryNodes = DiscoveryNodes.builder().add(indexNode).localNodeId(indexNode.getId()).masterNodeId(indexNode.getId());

        var indexRoutingTable = IndexRoutingTable.builder(shardId.getIndex()).addShard(primaryShard);

        for (int i = 0; i < searchNodes; i++) {
            var searchNode = DiscoveryNodeUtils.create(
                "search_node" + i,
                buildNewFakeTransportAddress(),
                Map.of(),
                Set.of(DiscoveryNodeRole.SEARCH_ROLE)
            );
            discoveryNodes.add(searchNode);

            indexRoutingTable.addShard(
                shardRoutingBuilder(shardId, searchNode.getId(), false, ShardRoutingState.STARTED).withRole(ShardRouting.Role.SEARCH_ONLY)
                    .build()
            );
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes.build())
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder(shardId.getIndex().getName())
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            )
                    )
                    .build()
            )
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    private ArrayList<String> returnInternalFiles(FakeStatelessNode testHarness, List<String> compoundCommitFiles) throws IOException {
        ArrayList<String> filesOnObjectStore = new ArrayList<>();
        for (String commitFile : compoundCommitFiles) {
            BlobContainer blobContainer = testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm);
            try (InputStream inputStream = blobContainer.readBlob(randomFrom(OperationPurpose.values()), commitFile)) {
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(new InputStreamStreamInput(inputStream));
                compoundCommit.commitFiles()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue().blobName().equals(commitFile))
                    .forEach(e -> filesOnObjectStore.add(e.getKey()));

            }
        }
        return filesOnObjectStore;
    }

    private static CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> fileCapture(Set<String> uploadedFiles) {
        return (s, r) -> {
            r.run();
            uploadedFiles.add(s);
        };
    }

    private FakeStatelessNode createNode(
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> commitFileConsumer,
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileConsumer
    ) throws IOException {
        int maxCommits = randomBoolean() ? STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getDefault(Settings.EMPTY) : randomIntBetween(1, 10);
        return createNode(commitFileConsumer, compoundCommitFileConsumer, maxCommits);
    }

    private FakeStatelessNode createNode(
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> commitFileConsumer,
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileConsumer,
        int maxCommits
    ) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {

                class WrappedBlobContainer extends FilterBlobContainer {
                    WrappedBlobContainer(BlobContainer delegate) {
                        super(delegate);
                    }

                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return new WrappedBlobContainer(child);
                    }

                    @Override
                    public void writeBlob(
                        OperationPurpose purpose,
                        String blobName,
                        InputStream inputStream,
                        long blobSize,
                        boolean failIfAlreadyExists
                    ) throws IOException {
                        assertFalse(blobName, blobName.startsWith(IndexFileNames.SEGMENTS));
                        commitFileConsumer.accept(
                            blobName,
                            () -> super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists)
                        );
                    }

                    @Override
                    public void writeMetadataBlob(
                        OperationPurpose purpose,
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        throw new AssertionError("writeMetadataBlob should not be called");
                    }

                    @Override
                    public void writeBlobAtomic(
                        OperationPurpose purpose,
                        String blobName,
                        InputStream inputStream,
                        long blobSize,
                        boolean failIfAlreadyExists
                    ) throws IOException {
                        assertTrue(blobName, StatelessCompoundCommit.startsWithBlobPrefix(blobName));
                        compoundCommitFileConsumer.accept(
                            blobName,
                            () -> super.writeBlobAtomic(purpose, blobName, inputStream, blobSize, failIfAlreadyExists)
                        );
                    }

                    @Override
                    public void writeBlobAtomic(
                        OperationPurpose purpose,
                        String blobName,
                        BytesReference bytes,
                        boolean failIfAlreadyExists
                    ) {
                        throw new AssertionError("writeBlobAtomic with BytesReference should not be called");
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }

            @Override
            protected Settings nodeSettings() {
                return Settings.builder().put(super.nodeSettings()).put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxCommits).build();
            }
        };
    }

    private static void markDeletedAndLocalUnused(
        List<StatelessCommitRef> usedCommits,
        List<StatelessCommitRef> unusedCommits,
        StatelessCommitService commitService,
        ShardId shardId
    ) {
        var indexEngineLocalReaderListenerForShard = commitService.getIndexEngineLocalReaderListenerForShard(shardId);
        Map<Long, Set<PrimaryTermAndGeneration>> openReaders = Stream.concat(usedCommits.stream(), unusedCommits.stream())
            .collect(
                Collectors.toMap(
                    FilterIndexCommit::getGeneration,
                    commit -> resolveBCCDependenciesForCommit(commitService, shardId, getPrimaryTermAndGenerationForCommit(commit))
                )
            );

        for (StatelessCommitRef indexCommit : unusedCommits) {
            var referencedBCCs = openReaders.remove(indexCommit.getGeneration());
            assert referencedBCCs.isEmpty() == false;

            Set<PrimaryTermAndGeneration> openLocalBCCs = openReaders.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
            long bccHoldingCommit = referencedBCCs.stream().max(PrimaryTermAndGeneration::compareTo).get().generation();

            boolean deleteFirst = randomBoolean();
            if (deleteFirst == false) {
                indexEngineLocalReaderListenerForShard.onLocalReaderClosed(bccHoldingCommit, openLocalBCCs);
            }
            commitService.markCommitDeleted(shardId, indexCommit.getGeneration());
            if (deleteFirst || randomBoolean()) {
                indexEngineLocalReaderListenerForShard.onLocalReaderClosed(bccHoldingCommit, openLocalBCCs);
            }
        }
    }

    private static PrimaryTermAndGeneration getPrimaryTermAndGenerationForCommit(StatelessCommitRef initialCommit) {
        return new PrimaryTermAndGeneration(initialCommit.getPrimaryTerm(), initialCommit.getGeneration());
    }

    private static Set<PrimaryTermAndGeneration> resolveBCCDependenciesForCommit(
        StatelessCommitService commitService,
        ShardId shardId,
        PrimaryTermAndGeneration commit
    ) {
        return commitService.getCommitBCCResolverForShard(shardId).resolveReferencedBCCsForCommit(commit.generation());
    }

    private void randomlyUseInitializingEmpty(FakeStatelessNode testHarness) {
        if (randomBoolean()) {
            testHarness.commitService.closeShard(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);
            testHarness.commitService.register(testHarness.shardId, primaryTerm, () -> true, (checkpoint, gcpListener, timeout) -> {
                gcpListener.accept(Long.MAX_VALUE, null);
            }, () -> {});
        }
    }

    private static ObjectStoreService.IndexingShardState readIndexingShardState(FakeStatelessNode node, long primaryTerm) {
        var future = new PlainActionFuture<ObjectStoreService.IndexingShardState>();
        ObjectStoreService.readIndexingShardState(
            IndexBlobStoreCacheDirectory.unwrapDirectory(node.indexingDirectory),
            IOContext.DEFAULT,
            node.objectStoreService.getBlobContainer(node.shardId),
            primaryTerm,
            node.threadPool,
            randomBoolean(),
            future
        );
        return safeGet(future);
    }
}
