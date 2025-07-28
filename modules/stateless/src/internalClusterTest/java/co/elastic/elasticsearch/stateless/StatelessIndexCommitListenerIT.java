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

import co.elastic.elasticsearch.stateless.commits.CommitBCCResolver;
import co.elastic.elasticsearch.stateless.commits.IndexEngineLocalReaderListener;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.recovery.RegisterCommitResponse;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toCollection;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class StatelessIndexCommitListenerIT extends AbstractStatelessIntegTestCase {

    /**
     * A testing stateless plugin that registers an {@link Engine.IndexCommitListener} that captures created and deleted commits.
     */
    public static class TestStateless extends Stateless {

        private final Map<ShardId, Map<Long, Engine.IndexCommitRef>> retainedCommits = new HashMap<>();
        private final Map<ShardId, Set<Long>> deletedCommits = new HashMap<>();
        private final Object mutex = new Object();

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected StatelessCommitService wrapStatelessCommitService(StatelessCommitService instance) {
            StatelessCommitService commitService = spy(instance);
            doAnswer(invocation -> {
                ActionListener<Void> argument = invocation.getArgument(2);
                argument.onResponse(null);
                return null;
            }).when(commitService).addListenerForUploadedGeneration(any(ShardId.class), anyLong(), any());
            // #onNewCommit is intercepted by the test, meaning that the ShardCommitState does not populate the data structures used for
            // file deletions; in this case we just mock the methods taking care of that tracking to be no-ops.
            doAnswer(invocation -> (CommitBCCResolver) generation -> Set.of(new PrimaryTermAndGeneration(1, generation))).when(
                commitService
            ).getCommitBCCResolverForShard(any(ShardId.class));
            doAnswer(invocation -> (IndexEngineLocalReaderListener) (bccHoldingClosedCommit, remainingReferencedBCCs) -> {}).when(
                commitService
            ).getIndexEngineLocalReaderListenerForShard(any(ShardId.class));
            doAnswer(invocation -> null).when(commitService).ensureMaxGenerationToUploadForFlush(any(ShardId.class), anyLong());
            doAnswer(invocation -> {
                PrimaryTermAndGeneration compoundCommitGeneration = invocation.getArgument(1);
                @SuppressWarnings("unchecked")
                ActionListener<RegisterCommitResponse> listener = invocation.getArgument(5);
                listener.onResponse(new RegisterCommitResponse(compoundCommitGeneration, null));
                return null;
            }).when(commitService)
                .registerCommitForUnpromotableRecovery(
                    any(PrimaryTermAndGeneration.class),
                    any(PrimaryTermAndGeneration.class),
                    any(ShardId.class),
                    anyString(),
                    any(ClusterState.class),
                    any()
                );
            return commitService;
        }

        @Override
        protected Engine.IndexCommitListener createIndexCommitListener() {
            return new Engine.IndexCommitListener() {

                @Override
                public void onNewCommit(
                    ShardId shardId,
                    Store store,
                    long primaryTerm,
                    Engine.IndexCommitRef indexCommitRef,
                    Set<String> additionalFiles
                ) {
                    synchronized (mutex) {
                        Map<Long, Engine.IndexCommitRef> commits = retainedCommits.computeIfAbsent(shardId, s -> new HashMap<>());
                        var previous = commits.put(indexCommitRef.getIndexCommit().getGeneration(), indexCommitRef);
                        assertThat("Commit already exists " + indexCommitRef.getIndexCommit().getGeneration(), previous, nullValue());
                    }
                }

                @Override
                public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {
                    synchronized (mutex) {
                        var previous = deletedCommits.computeIfAbsent(shardId, s -> new HashSet<>()).add(deletedCommit.getGeneration());
                        assertThat("Commit already deleted: " + deletedCommit, previous, is(true));
                    }
                }
            };
        }

        private List<Long> listRetainedCommits(ShardId shardId) {
            synchronized (mutex) {
                return retainedCommits.getOrDefault(shardId, Map.of()).keySet().stream().sorted().toList();
            }
        }

        private List<Long> listDeletions(ShardId shardId) {
            synchronized (mutex) {
                return deletedCommits.getOrDefault(shardId, Set.of()).stream().sorted().toList();
            }
        }

        private Engine.IndexCommitRef getIndexCommitRef(ShardId shardId, long generation) {
            synchronized (mutex) {
                Map<Long, Engine.IndexCommitRef> shardCommits = retainedCommits.get(shardId);
                if (shardCommits == null || shardCommits.isEmpty()) {
                    throw new AssertionError("No commits for shard " + shardId);
                }
                Engine.IndexCommitRef commitRef = shardCommits.get(generation);
                if (commitRef == null) {
                    throw new AssertionError("Commit with generation " + generation + " not found for shard " + shardId);
                }
                return commitRef;
            }
        }

        private void release(ShardId shardId, long generation) {
            synchronized (mutex) {
                try {
                    Map<Long, Engine.IndexCommitRef> shardCommits = retainedCommits.get(shardId);
                    if (shardCommits == null || shardCommits.isEmpty()) {
                        throw new AssertionError("No commits for shard " + shardId);
                    }
                    Engine.IndexCommitRef commitRef = shardCommits.remove(generation);
                    if (commitRef == null) {
                        throw new AssertionError("Commit with generation " + generation + " not found for shard " + shardId);
                    }
                    commitRef.close();
                } catch (IOException e) {
                    throw new AssertionError("Failed to release commit with generation " + generation + " on shard " + shardId + e);
                }
            }
        }

        @Override
        public void close() throws IOException {
            synchronized (mutex) {
                retainedCommits.clear();
                deletedCommits.clear();
            }
            super.close();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // tests in this suite expect a precise number of commits
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    private String indexNode;
    private String searchNode;
    private String indexName;
    private ShardId shardId;

    @Before
    public void setupTest() {
        startMasterOnlyNode();
        indexNode = startIndexNode();
        searchNode = startSearchNode();

        indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1)
                // tests control flushes
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );
        ensureGreen(indexName);
        shardId = new ShardId(resolveIndex(indexName), 0);

        assertCommitsGenerations("Shard has a single commit with generation 3 after creation", List.of(3L), List.of());
    }

    @After
    public void teardownTest() {
        assertAcked(client().admin().indices().prepareDelete(indexName));
        indexNode = null;
        searchNode = null;
        indexName = null;
    }

    public void testCommits() {
        indexDocs(indexName, scaledRandomIntBetween(10, 1_000));
        flush(indexName);
        assertCommitsGenerations("New retained commit 4 after flush and commit 3 not yet released", List.of(3L, 4L), List.of());

        releaseCommit(3L);
        assertCommitsGenerations(
            "Commit 3 was released and it will be deleted by Lucene during the next flush and commit 4 is still retained",
            List.of(4L),
            List.of()
        );

        releaseCommit(4L);
        assertCommitsGenerations(
            "No more commits retained and commit 4 not deleted because it is the last commit "
                + "and commit 3 is still not deleted because there has not been any flushes",
            List.of(),
            List.of()
        );

        indexDocs(indexName, scaledRandomIntBetween(10, 1_000));
        flush(indexName);
        assertCommitsGenerations("New retained commit 5 after flush and commit 4 deleted by Lucene", List.of(5L), List.of(3L, 4L));

        releaseCommit(5L);
        assertCommitsGenerations("No more commits retained and commit 5 is the last commit", List.of(), List.of(3L, 4L));
    }

    public void testCommitsWithUnorderedReleases() {
        final int nbGenerations = randomIntBetween(2, 20);
        final long initialGeneration = 3L;
        for (long i = 1L; i <= nbGenerations; i++) {
            indexDocs(indexName, scaledRandomIntBetween(10, 100));
            flush(indexName);
            assertCommitsGenerations(
                "New commit " + i,
                LongStream.rangeClosed(initialGeneration, initialGeneration + i).boxed().toList(),
                List.of()
            );
        }

        final List<Long> unreleasedGens = LongStream.rangeClosed(initialGeneration, initialGeneration + nbGenerations)
            .boxed()
            .collect(toCollection(ArrayList::new));
        Randomness.shuffle(unreleasedGens);

        final List<Long> deletedGens = new ArrayList<>();
        while (unreleasedGens.isEmpty() == false) {
            long releasedGen = unreleasedGens.remove(0);
            releaseCommit(releasedGen);
            if (releasedGen != nbGenerations + initialGeneration) {
                deletedGens.add(releasedGen);
            }
            assertCommitsGenerations(
                "Releasing commits",
                unreleasedGens.stream().sorted().toList(),
                // The deletion policy is not revisited until there's a flush
                List.of()
            );
        }
        // Force a flush so the deletion policy is revisited and the released commits are marked as deleted
        indicesAdmin().prepareFlush(indexName).setForce(true).get();
        // Release the newly created commit immediately
        releaseCommit(initialGeneration + nbGenerations + 1);
        assertCommitsGenerations(
            "End of test",
            List.of(),
            // Notice that the newly created commit won't be marked as deleted until the next flush
            LongStream.rangeClosed(initialGeneration, initialGeneration + nbGenerations).boxed().sorted().toList()
        );
    }

    public void testMerges() throws Exception {
        final int nbMerges = randomIntBetween(1, 5);
        while (true) {
            indexDocs(indexName, 10);
            flush(indexName);

            var mergesResponse = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get();
            var primaries = mergesResponse.getIndices().get(indexName).getPrimaries();
            if (primaries.merge.getTotal() >= nbMerges) {
                break;
            }
        }

        var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        var indexShard = indexService.getShard(shardId.id());

        var plugin = getStatelessPluginInstance();
        final List<Long> generations = new ArrayList<>(plugin.listRetainedCommits(shardId));

        // retrieve the list of files that are included in the latest commit
        final Engine.IndexCommitRef lastCommitRef = plugin.getIndexCommitRef(shardId, generations.get(generations.size() - 1));
        final Set<String> lastCommitFiles = Set.copyOf(lastCommitRef.getIndexCommit().getFileNames());

        // builds a map of all files names with the list of commit generations they belong to
        final Map<String, Set<Long>> allFilesWithGenerations = new HashMap<>();
        for (long generation : generations) {
            var commitRef = plugin.getIndexCommitRef(shardId, generation);
            var commit = commitRef.getIndexCommit();
            commit.getFileNames().forEach(f -> allFilesWithGenerations.computeIfAbsent(f, s -> new HashSet<>()).add(generation));
        }

        Collections.shuffle(generations, random());

        // release commit in random order and check that the files that are not referenced anymore are effectively deleted from disk
        for (long generation : generations) {
            var commitRef = plugin.getIndexCommitRef(shardId, generation);
            assertThat(commitRef.getIndexCommit().getGeneration(), equalTo(generation));
            plugin.release(shardId, generation);

            var commitFiles = Set.copyOf(commitRef.getIndexCommit().getFileNames());
            for (String commitFile : commitFiles) {
                Set<Long> fileGenerations = allFilesWithGenerations.get(commitFile);
                assertThat(fileGenerations.remove(generation), equalTo(true));
                // The policy is not revisited until there's a flush, so the files must be retained on disk even if the commit is released
                assertThat(
                    "File " + commitFile + " of commit generation " + generation,
                    Files.exists(indexShard.shardPath().resolveIndex().resolve(commitFile)),
                    equalTo(true)
                );
            }
        }

        // Force a flush so the deletion policy is revisited
        indicesAdmin().prepareFlush(indexName).setForce(true).get();

        var lastFlushGeneration = plugin.listRetainedCommits(shardId).getLast();
        final var lastFlushCommitRef = plugin.getIndexCommitRef(shardId, lastFlushGeneration);
        final var lastFlushCommitFiles = new HashSet<>(lastFlushCommitRef.getIndexCommit().getFileNames());

        // At this point all the original files that are not referenced by lastFlushCommit must be deleted locally
        for (var entry : allFilesWithGenerations.entrySet()) {
            if (lastFlushCommitFiles.contains(entry.getKey())) {
                assertThat(Files.exists(indexShard.shardPath().resolveIndex().resolve(entry.getKey())), equalTo(true));
            } else {
                assertThat(Files.exists(indexShard.shardPath().resolveIndex().resolve(entry.getKey())), equalTo(false));
            }
            assertThat(entry.getValue(), emptyIterable());
        }

        releaseCommit(lastFlushGeneration);
    }

    private TestStateless getStatelessPluginInstance() {
        var plugin = internalCluster().getInstance(PluginsService.class, indexNode).filterPlugins(TestStateless.class).findFirst().get();
        assertThat("TestStateless plugin not found on node " + indexNode, plugin, notNullValue());
        return plugin;
    }

    private void assertCommitsGenerations(String message, List<Long> expectedRetainedGenerations, List<Long> expectedDeletedGenerations) {
        var plugin = getStatelessPluginInstance();
        assertThat(
            "Retained commits (" + message + ')',
            plugin.listRetainedCommits(shardId),
            expectedRetainedGenerations.isEmpty() ? emptyIterable() : equalTo(expectedRetainedGenerations)
        );
        assertThat(
            "Deleted commits (" + message + ')',
            plugin.listDeletions(shardId),
            expectedDeletedGenerations.isEmpty() ? emptyIterable() : equalTo(expectedDeletedGenerations)
        );
    }

    private void releaseCommit(long generation) {
        var plugin = getStatelessPluginInstance();
        plugin.release(shardId, generation);
    }
}
