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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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

    public void testCommitUpload() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(3, 8));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
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

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(2, 4));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> blobNameFromGeneration(ref.getGeneration())).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
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
                try {
                    startingUpload.countDown();
                    blocking.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertFalse(uploadedBlobs.contains(secondCommitFile.get()));
            } else {
                assertEquals(compoundCommitFile, secondCommitFile.get());
                assertTrue(uploadedBlobs.contains(firstCommitFile.get()));
            }
            compoundCommitFileCapture.accept(compoundCommitFile, runnable);
        })) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, 2);
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
            startingUpload.await();
            assertThat(uploadedBlobs, not(hasItems(commitFileToBlock.get())));
            assertThat(uploadedBlobs, not(hasItems(firstCommitFile.get())));

            testHarness.commitService.onCommitCreation(secondCommit);

            assertThat(uploadedBlobs, not(hasItems(secondCommitFile.get())));

            blocking.countDown();

            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
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

    public void testMapIsPrunedOnIndexDelete() throws Exception {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            List<StatelessCommitRef> refs = generateIndexCommits(testHarness, 2, true);
            StatelessCommitRef firstCommit = refs.get(0);
            StatelessCommitRef secondCommit = refs.get(1);

            testHarness.commitService.onCommitCreation(firstCommit);

            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future);
            future.actionGet();

            testHarness.commitService.onCommitCreation(secondCommit);

            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
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
            testHarness.commitService.closedLocalReadersForGeneration(testHarness.shardId).accept(firstCommit.getGeneration());

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
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            StatelessCommitRef commitRef = generateIndexCommits(testHarness, 1).get(0);

            List<String> commitFiles = commitRef.getCommitFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
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

            var indexingShardState = ObjectStoreService.readIndexingShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                primaryTerm
            );

            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId, primaryTerm);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, indexingShardState.v1(), indexingShardState.v2());

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future2);
            future2.actionGet();

            assertThat(uploadedBlobs, empty());
        }
    }

    public void testWaitForGenerationFailsForClosedShard() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            ShardId unregisteredShardId = new ShardId("index", "uuid", 0);
            expectThrows(
                AlreadyClosedException.class,
                () -> testHarness.commitService.addListenerForUploadedGeneration(unregisteredShardId, 1, PlainActionFuture.newFuture())
            );
            PlainActionFuture<Void> failedFuture = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, 1, failedFuture);

            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, failedFuture::actionGet);
        }
    }

    public void testCommitsTrackingTakesIntoAccountSearchNodeUsage() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = staleCommits(initialCommits, shardId);
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOldCommitsAreRetainedIfSearchNodesUseThem() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            var firstCommit = initialCommits.get(0);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, firstCommit.getGeneration()),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            // Commits 0-9 are reported to be used by the search node;
            // therefore we should retain them even if the indexing node has deleted them
            assertThat(deletedCommits, empty());

            // Now the search shard, report that it uses commit 0 and commit 10; therefore we should delete commits [1, 8]
            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
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
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            var firstCommit = initialCommits.get(0);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            for (StatelessCommitRef indexCommit : initialCommits) {
                commitService.markCommitDeleted(shardId, indexCommit.getGeneration());
            }

            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, mergedCommit.getGeneration(), future);
            future.actionGet();

            // Commits 0-9 are still used by the indexing shard
            // therefore we should retain them even if the indexing node/lucene has deleted them
            assertThat(deletedCommits, empty());

            // Now the index shard, report that it no longer uses commits [1;9] and they should be deleted
            LongConsumer closedLocalReadersForGeneration = commitService.closedLocalReadersForGeneration(shardId);
            initialCommits.stream()
                .filter(c -> c.getGeneration() != firstCommit.getGeneration())
                .forEach(c -> closedLocalReadersForGeneration.accept(c.getGeneration()));

            var expectedDeletedCommits = initialCommits.stream()
                .map(commit -> staleCommit(shardId, commit))
                .filter(commit -> commit.primaryTermAndGeneration().generation() != firstCommit.getGeneration())
                .collect(Collectors.toSet());
            // the external reader notification and thus delete can go on after the upload completed.
            assertBusy(() -> { assertThat(deletedCommits, equalTo(expectedDeletedCommits)); });
        }
    }

    public void testRetainedCommitsAreReleasedAfterANodeIsUnassigned() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            assertThat(deletedCommits, empty());

            // The search node is still using commit 9 and the merged commit; therefore we should keep all commits around
            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
                new PrimaryTermAndGeneration(
                    initialCommits.get(initialCommits.size() - 1).getPrimaryTerm(),
                    initialCommits.get(initialCommits.size() - 1).getGeneration()
                ),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            assertThat(deletedCommits, empty());

            var unassignedSearchShard = TestShardRouting.newShardRouting(
                shardId,
                null,
                null,
                false,
                ShardRoutingState.UNASSIGNED,
                ShardRouting.Role.SEARCH_ONLY
            );

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

            commitService.clusterChanged(new ClusterChangedEvent("unassigned search shard", clusterStateWithUnassignedSearchShard, state));

            var expectedDeletedCommits = staleCommits(initialCommits, shardId);
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testOutOfOrderNewCommitNotifications() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 2);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            assertThat(deletedCommits, empty());

            // The search shard is using the latest commit, but there are concurrent in-flight notifications
            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            if (randomBoolean()) {
                commitService.markCommitDeleted(shardId, mergedCommit.getGeneration());
            }

            for (StatelessCommitRef initialCommit : initialCommits) {
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            // There were multiple in-flight new commit notification responses, but the notification filter by generation should ensure
            // we still delete all initial files.
            assertThat(deletedCommits, equalTo(staleCommits(initialCommits, shardId)));
        }
    }

    public void testCommitsAreReleasedImmediatelyAfterDeletionWhenThereAreZeroReplicas() throws Exception {
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
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                assert stateRef.get() != null;
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NoOpNodeClient(getTestName()) {
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

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            stateRef.set(state);

            var initialCommits = generateIndexCommits(testHarness, 2);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            PlainActionFuture<Void> mergedCommitUploadedFuture = new PlainActionFuture<>();
            commitService.addListenerForUploadedGeneration(shardId, mergedCommit.getGeneration(), mergedCommitUploadedFuture);
            mergedCommitUploadedFuture.actionGet();

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            var expectedDeletedCommits = staleCommits(initialCommits, shardId);
            assertBusy(() -> assertThat(deletedCommits, is(equalTo(expectedDeletedCommits))));
        }
    }

    public void testCommitsAreSuccessfullyInitializedAfterRecovery() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var indexingShardState = ObjectStoreService.readIndexingShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                primaryTerm
            );

            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId, primaryTerm);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, indexingShardState.v1(), indexingShardState.v2());

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            StatelessCommitRef recoveryCommit = initialCommits.get(initialCommits.size() - 1);
            assert recoveryCommit.getGeneration() == indexingShardState.v1().generation();
            commitService.markCommitDeleted(shardId, recoveryCommit.getGeneration());
            commitService.closedLocalReadersForGeneration(shardId).accept(recoveryCommit.getGeneration());

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = staleCommits(initialCommits, shardId);
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));
        }
    }

    public void testUnreferencedCommitsAreReleasedAfterRecovery() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
                new PrimaryTermAndGeneration(primaryTerm, mergedCommit.getGeneration())
            );

            testHarness.commitService.unregister(testHarness.shardId);

            var indexingShardState = ObjectStoreService.readIndexingShardState(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId),
                primaryTerm
            );

            testHarness.commitService.register(testHarness.shardId, primaryTerm);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, indexingShardState.v1(), indexingShardState.v2());

            StatelessCommitRef recoveryCommit = mergedCommit;
            assert recoveryCommit.getGeneration() == indexingShardState.v1().generation();

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            StatelessCommitRef newCommit = generateIndexCommits(testHarness, 1, true).get(0);
            StatelessCommitRef retainedBySearch = initialCommits.get(1);
            commitService.onCommitCreation(newCommit);
            fakeSearchNode.respondWithUsedCommits(
                newCommit.getGeneration(),
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
            assertThat(deletedCommits, equalTo(expectedDeletedCommits));

            commitService.markCommitDeleted(shardId, recoveryCommit.getGeneration());
            commitService.closedLocalReadersForGeneration(shardId).accept(recoveryCommit.getGeneration());
            HashSet<StaleCompoundCommit> newDeleted = new HashSet<>(expectedDeletedCommits);
            newDeleted.add(staleCommit(shardId, recoveryCommit));
            assertThat(deletedCommits, equalTo(newDeleted));
        }
    }

    public void testRegisterUnpromotableRecovery() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
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
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
            }

            @Override
            protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
                return new NoOpNodeClient(getTestName()) {
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

                    @Override
                    public void close() {
                        super.close();
                        fakeSearchNode.close();
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

            var initialCommits = generateIndexCommits(testHarness, 10);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }

            var commit = initialCommits.get(initialCommits.size() - 1);
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commit.getGeneration(), future);
            future.actionGet();

            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(state);
            activateSearchNode.set(true);

            boolean clusterChangedFirst = randomBoolean();
            if (clusterChangedFirst) {
                commitService.clusterChanged(new ClusterChangedEvent("test", state, stateWithNoSearchShards));
            }
            PlainActionFuture<PrimaryTermAndGeneration> registerFuture = new PlainActionFuture<>();
            commitService.registerCommitForUnpromotableRecovery(
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
            PrimaryTermAndGeneration registeredCommit = registerFuture.actionGet();
            assertThat(registeredCommit, equalTo(new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration())));

            var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
            commitService.onCommitCreation(mergedCommit);

            markDeletedAndLocalUnused(initialCommits, commitService, shardId);

            // The search node is still using commit 9, that contains references to all previous commits;
            // therefore we should retain all commits.
            assertThat(deletedCommits, empty());

            fakeSearchNode.respondWithUsedCommits(
                mergedCommit.getGeneration(),
                new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration())
            );

            var expectedDeletedCommits = staleCommits(initialCommits, shardId);
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
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            int numberOfCommits = randomIntBetween(1, 10);
            var initialCommits = generateIndexCommits(testHarness, numberOfCommits);
            var delayedNewCommitNotifications = randomSubsetOf(initialCommits);
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
                if (delayedNewCommitNotifications.contains(initialCommit) == false) {
                    fakeSearchNode.respondWithUsedCommits(
                        initialCommit.getGeneration(),
                        new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                    );
                }
            }

            // Wait for sending all new commit notification requests, to be able to respond to them.
            assertBusy(() -> assertThat(fakeSearchNode.generationPendingListeners.size(), equalTo(numberOfCommits)));

            testHarness.commitService.delete(testHarness.shardId);
            testHarness.commitService.unregister(testHarness.shardId);

            for (StatelessCommitRef initialCommit : delayedNewCommitNotifications) {
                fakeSearchNode.respondWithUsedCommits(
                    initialCommit.getGeneration(),
                    new PrimaryTermAndGeneration(primaryTerm, initialCommit.getGeneration())
                );
            }

            // All commits must be deleted
            var expectedDeletedCommits = staleCommits(initialCommits, shardId);
            assertBusy(() -> { assertThat(deletedCommits, is(expectedDeletedCommits)); });
        }
    }

    public void testConcurrentIndexingSearchAndRecoveries() throws Exception {
        Set<StaleCompoundCommit> deletedCommits = ConcurrentCollections.newConcurrentSet();
        var fakeSearchNode = new FakeSearchNode("fakeSearchNode");
        var commitCleaner = new StatelessCommitCleaner(null, null, null) {
            @Override
            void deleteCommit(StaleCompoundCommit staleCompoundCommit) {
                deletedCommits.add(staleCompoundCommit);
            }
        };
        var stateRef = new AtomicReference<ClusterState>();
        try (var testHarness = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
                return stateRef.get().routingTable().shardRoutingTable(shardId);
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

            var noSearchState = clusterStateWithPrimaryAndSearchShards(shardId, 0);
            var searchState = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            stateRef.set(noSearchState);

            CyclicBarrier barrier = new CyclicBarrier(3);
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong indexingRoundsCompleted = new AtomicLong();

            List<StatelessCommitRef> initialCommits = generateIndexCommits(testHarness, between(1, 5));
            initialCommits.forEach(commitService::onCommitCreation);
            Set<PrimaryTermAndGeneration> allCommits = ConcurrentCollections.newConcurrentSet();
            initialCommits.stream()
                .map(commit -> new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration()))
                .forEach(allCommits::add);

            StatelessCommitRef initialLatestCommit = initialCommits.get(initialCommits.size() - 1);
            AtomicReference<PrimaryTermAndGeneration> latestCommit = new AtomicReference<>(
                new PrimaryTermAndGeneration(initialLatestCommit.getPrimaryTerm(), initialLatestCommit.getGeneration())
            );

            Thread indexer = new Thread(() -> {
                List<StatelessCommitRef> previous = initialCommits;
                safeAwait(barrier);
                try {
                    while (running.get()) {
                        List<StatelessCommitRef> newCommits = generateIndexCommits(testHarness, between(1, 5));
                        newCommits.forEach(commitService::onCommitCreation);
                        newCommits.stream()
                            .map(commit -> new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration()))
                            .forEach(allCommits::add);
                        if (randomBoolean()) {
                            StatelessCommitRef randomCommit = randomFrom(newCommits);
                            latestCommit.set(new PrimaryTermAndGeneration(randomCommit.getPrimaryTerm(), randomCommit.getGeneration()));
                        }
                        markDeletedAndLocalUnused(previous, commitService, shardId);
                        previous = newCommits;
                        Thread.yield();
                        indexingRoundsCompleted.incrementAndGet();
                    }
                    var mergedCommit = generateIndexCommits(testHarness, 1, true).get(0);
                    commitService.onCommitCreation(mergedCommit);
                    latestCommit.set(new PrimaryTermAndGeneration(mergedCommit.getPrimaryTerm(), mergedCommit.getGeneration()));
                    fakeSearchNode.respondWithUsedCommits(mergedCommit.getGeneration(), latestCommit.get());

                    markDeletedAndLocalUnused(previous, commitService, shardId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, "indexer");

            Thread searcher = new Thread(new Runnable() {
                long generationResponded = latestCommit.get().generation();

                public void respond(long toGeneration, PrimaryTermAndGeneration... commits) {
                    while (generationResponded < toGeneration) {
                        ++generationResponded;
                        // we generate commits with holes in the sequence, skip those.
                        if (allCommits.contains(new PrimaryTermAndGeneration(primaryTerm, generationResponded))) {
                            fakeSearchNode.respondWithUsedCommits(generationResponded, commits);
                        }
                    }
                }

                @Override
                public void run() {
                    safeAwait(barrier);
                    for (long i = 0; i < 10 || indexingRoundsCompleted.get() < 10; ++i) {
                        respond(latestCommit.get().generation());
                        var clusterState = changeClusterState(searchState);
                        PlainActionFuture<PrimaryTermAndGeneration> future = new PlainActionFuture<>();
                        commitService.registerCommitForUnpromotableRecovery(
                            latestCommit.get(),
                            shardId,
                            searchState.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0).currentNodeId(),
                            clusterState,
                            future
                        );
                        PrimaryTermAndGeneration recoveryCommit;
                        try {
                            recoveryCommit = future.actionGet();
                        } catch (NoShardAvailableActionException e) {
                            // todo: avoid the NoShardAvailableException when not warranted.
                            continue;
                        }
                        if (randomBoolean()) {
                            respond(recoveryCommit.generation() - 1);
                        } else if (randomBoolean()) {
                            respond(recoveryCommit.generation() - 1, recoveryCommit);
                        }

                        if (randomBoolean()) {
                            respond(latestCommit.get().generation(), recoveryCommit);
                        } else {
                            // respond filters duplicates
                            PrimaryTermAndGeneration latest = latestCommit.get();
                            respond(latest.generation(), recoveryCommit, latest);
                            assertThat(deletedCommits, not(hasItems(new StaleCompoundCommit(shardId, latest, primaryTerm))));
                        }
                        assertThat(deletedCommits, not(hasItems(new StaleCompoundCommit(shardId, recoveryCommit, primaryTerm))));

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
            safeAwait(barrier);
            searcher.join();
            running.set(false);
            indexer.join();
            var expectedDeletedCommits = allCommits.stream()
                .map(commit -> new StaleCompoundCommit(shardId, commit, primaryTerm))
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
            var state = clusterStateWithPrimaryAndSearchShards(shardId, 1);
            var initialCommits = generateIndexCommits(testHarness, 10);
            var commit = initialCommits.get(initialCommits.size() - 1);
            var nodeId = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0).currentNodeId();

            commitService.clusterChanged(new ClusterChangedEvent("test", state, stateWithNoSearchShards));
            // Registration sent to an un-initialized shard
            var commitToRegister = new PrimaryTermAndGeneration(commit.getPrimaryTerm(), commit.getGeneration());
            PlainActionFuture<PrimaryTermAndGeneration> registerFuture = new PlainActionFuture<>();
            commitService.registerCommitForUnpromotableRecovery(commitToRegister, shardId, nodeId, state, registerFuture);
            expectThrows(NoShardAvailableActionException.class, registerFuture::actionGet);
            // Registering after initialization is done should work
            for (StatelessCommitRef initialCommit : initialCommits) {
                commitService.onCommitCreation(initialCommit);
            }
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commit.getGeneration(), future);
            future.actionGet();
            registerFuture = new PlainActionFuture<>();
            commitService.registerCommitForUnpromotableRecovery(commitToRegister, shardId, nodeId, state, registerFuture);
            assertThat(registerFuture.get(), equalTo(commitToRegister));
        }
    }

    private Set<StaleCompoundCommit> staleCommits(List<StatelessCommitRef> commits, ShardId shardId) {
        return commits.stream().map(commit -> staleCommit(shardId, commit)).collect(Collectors.toSet());
    }

    private StaleCompoundCommit staleCommit(ShardId shardId, StatelessCommitRef commit) {
        return new StaleCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, commit.getGeneration()), primaryTerm);
    }

    private static class FakeSearchNode extends NoOpNodeClient {
        private final Map<Long, PlainActionFuture<ActionListener<NewCommitNotificationResponse>>> generationPendingListeners =
            new HashMap<>();

        FakeSearchNode(String testName) {
            super(testName);
        }

        void respondWithUsedCommits(long generation, PrimaryTermAndGeneration... usedCommits) {
            // allow duplicates in usedCommits for now.
            getListenerForNewCommitNotification(generation).actionGet()
                .onResponse(new NewCommitNotificationResponse(Arrays.stream(usedCommits).collect(Collectors.toSet())));
        }

        private synchronized PlainActionFuture<ActionListener<NewCommitNotificationResponse>> getListenerForNewCommitNotification(
            long generation
        ) {
            PlainActionFuture<ActionListener<NewCommitNotificationResponse>> future = new PlainActionFuture<>();
            return generationPendingListeners.computeIfAbsent(generation, unused -> future);
        }

        synchronized void onNewNotification(NewCommitNotificationRequest request, ActionListener<NewCommitNotificationResponse> listener) {
            getListenerForNewCommitNotification(request.getGeneration()).onResponse(listener);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            onNewNotification((NewCommitNotificationRequest) request, (ActionListener<NewCommitNotificationResponse>) listener);
        }
    }

    private ClusterState clusterStateWithPrimaryAndSearchShards(ShardId shardId, int searchNodes) {
        var indexNode = DiscoveryNodeUtils.create(
            "index_node",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.MASTER_ROLE)
        );

        var primaryShard = TestShardRouting.newShardRouting(shardId, indexNode.getId(), true, ShardRoutingState.STARTED);

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
                TestShardRouting.newShardRouting(
                    shardId,
                    searchNode.getId(),
                    null,
                    false,
                    ShardRoutingState.STARTED,
                    ShardRouting.Role.SEARCH_ONLY
                )
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
            try (InputStream inputStream = blobContainer.readBlob(OperationPurpose.SNAPSHOT, commitFile)) {
                long fileLength = blobContainer.listBlobs(OperationPurpose.SNAPSHOT).get(commitFile).length();
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(
                    new InputStreamStreamInput(inputStream),
                    fileLength
                );
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
                        assertTrue(blobName, StatelessCompoundCommit.startsWithBlobPrefix(blobName));
                        compoundCommitFileConsumer.accept(
                            blobName,
                            () -> super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer)
                        );
                    }

                    @Override
                    public void writeBlobAtomic(
                        OperationPurpose purpose,
                        String blobName,
                        BytesReference bytes,
                        boolean failIfAlreadyExists
                    ) {
                        throw new AssertionError("should not be called");
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }
        };
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber) throws IOException {
        return generateIndexCommits(testHarness, commitsNumber, false);
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber, boolean merge)
        throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        Set<String> previousCommit;

        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        String deleteId = randomAlphaOfLength(10);

        try (var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig)) {
            try (var indexReader = DirectoryReader.open(indexWriter)) {
                IndexCommit indexCommit = indexReader.getIndexCommit();
                previousCommit = new HashSet<>(indexCommit.getFileNames());
            }
            for (int i = 0; i < commitsNumber; i++) {
                LuceneDocument document = new LuceneDocument();
                document.add(new KeywordField("field0", "term", Field.Store.YES));
                indexWriter.addDocument(document.getFields());
                if (randomBoolean()) {
                    final ParsedDocument tombstone = ParsedDocument.deleteTombstone(deleteId);
                    LuceneDocument delete = tombstone.docs().get(0);
                    NumericDocValuesField field = Lucene.newSoftDeletesField();
                    delete.add(field);
                    indexWriter.softUpdateDocument(EngineTestCase.newUid(deleteId), delete.getFields(), Lucene.newSoftDeletesField());
                }
                indexWriter.commit();
                if (merge) {
                    indexWriter.forceMerge(1, true);
                }
                try (var indexReader = DirectoryReader.open(indexWriter)) {
                    IndexCommit indexCommit = indexReader.getIndexCommit();
                    Set<String> commitFiles = new HashSet<>(indexCommit.getFileNames());
                    Set<String> additionalFiles = Sets.difference(commitFiles, previousCommit);
                    previousCommit = commitFiles;

                    StatelessCommitRef statelessCommitRef = new StatelessCommitRef(
                        testHarness.shardId,
                        new Engine.IndexCommitRef(indexCommit, () -> {}),
                        commitFiles,
                        additionalFiles,
                        primaryTerm,
                        0
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }

        return commits;
    }

    private static void markDeletedAndLocalUnused(List<StatelessCommitRef> commits, StatelessCommitService commitService, ShardId shardId) {
        LongConsumer closedLocalReadersForGeneration = commitService.closedLocalReadersForGeneration(shardId);
        for (StatelessCommitRef indexCommit : commits) {
            boolean deleteFirst = randomBoolean();
            if (deleteFirst == false) {
                closedLocalReadersForGeneration.accept(indexCommit.getGeneration());
            }
            commitService.markCommitDeleted(shardId, indexCommit.getGeneration());
            if (deleteFirst || randomBoolean()) {
                closedLocalReadersForGeneration.accept(indexCommit.getGeneration());
            }
        }
    }
}
