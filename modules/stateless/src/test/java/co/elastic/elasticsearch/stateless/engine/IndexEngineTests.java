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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.LongStream;

import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.EngineTestCase.newUid;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexEngineTests extends AbstractEngineTestCase {

    public void testAsyncEnsureSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            Translog.Location location = new Translog.Location(0, 0, 0);
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            doAnswer((Answer<Void>) invocation -> {
                future.onResponse(null);
                return null;
            }).when(replicator).sync(eq(engine.config().getShardId()), eq(location), any());
            engine.asyncEnsureTranslogSynced(location, e -> {
                if (e == null) {
                    future.onResponse(null);
                } else {
                    future.onFailure(e);
                }
            });
            verify(replicator).sync(eq(engine.config().getShardId()), eq(location), any());
            future.actionGet();
        }
    }

    public void testSync() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            doAnswer((Answer<Void>) invocation -> {
                ActionListener<Void> listener = invocation.getArgument(1);
                listener.onResponse(null);
                return null;
            }).when(replicator).syncAll(eq(engine.config().getShardId()), any());
            engine.syncTranslog();
            verify(replicator).syncAll(eq(engine.config().getShardId()), any());
        }
    }

    public void testSyncIsNeededIfTranslogReplicatorHasUnsyncedData() throws Exception {
        TranslogReplicator replicator = mock(TranslogReplicator.class);
        try (var engine = newIndexEngine(indexConfig(), replicator)) {
            assertFalse(engine.isTranslogSyncNeeded());
            when(replicator.isSyncNeeded(engine.config().getShardId())).thenReturn(true);
            assertTrue(engine.isTranslogSyncNeeded());
        }
    }

    public void testRefreshNeededForFastRefreshesBasedOnSearcher() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings
                )
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            assertFalse(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            engine.flush();
            assertTrue(engine.refreshNeeded());
        }
    }

    public void testRefreshNeededForNonFastRefreshesBasedOnSearcherAndCommits() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60)).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            // Need refresh because only maybeRefresh / externalRefreshes commit
            assertTrue(engine.refreshNeeded());
            engine.index(randomDoc(String.valueOf(1)));
            // Still need refresh
            assertTrue(engine.refreshNeeded());
            if (randomBoolean()) {
                PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
                engine.externalRefresh("test", future);
                future.actionGet();
            } else {
                PlainActionFuture<Engine.RefreshResult> future = new PlainActionFuture<>();
                engine.maybeRefresh("test", future);
                future.actionGet();
            }
            assertFalse(engine.refreshNeeded());
        }
    }

    public void testRefreshesWaitForUploadWithoutDelayed() throws IOException {
        Settings nodeSettings = Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), false)
            .build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            final var statelessCommitService = engine.getStatelessCommitService();
            assertThat(statelessCommitService.isStatelessUploadDelayed(), is(false));
            doTestRefreshesWaitForUploadBehaviours(engine);
        }
    }

    public void testRefreshesDoesNotWaitForUploadWithStatelessUploadDelayed() throws IOException {
        Settings nodeSettings = Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
            .build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            final var statelessCommitService = engine.getStatelessCommitService();
            assertThat(statelessCommitService.isStatelessUploadDelayed(), is(true));
            doTestRefreshesWaitForUploadBehaviours(engine);
        }
    }

    private void doTestRefreshesWaitForUploadBehaviours(IndexEngine engine) throws IOException {
        var statelessCommitService = engine.getStatelessCommitService();

        // External refresh
        engine.index(randomDoc(String.valueOf(0)));
        assertTrue(engine.refreshNeeded());
        var future = new PlainActionFuture<Engine.RefreshResult>();
        engine.externalRefresh("test", future);
        Engine.RefreshResult refreshResult = future.actionGet();
        assertFalse(engine.refreshNeeded());
        if (statelessCommitService.isStatelessUploadDelayed() == false) {
            verify(statelessCommitService, times(1)).addListenerForUploadedGeneration(
                any(),
                eq(refreshResult.generation()),
                anyActionListener()
            );
        } else {
            verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());
        }

        // Scheduled refresh
        engine.index(randomDoc(String.valueOf(1)));
        assertTrue(engine.refreshNeeded());
        future = new PlainActionFuture<>();
        engine.maybeRefresh("test", future);
        refreshResult = future.actionGet();
        assertFalse(engine.refreshNeeded());
        if (statelessCommitService.isStatelessUploadDelayed() == false) {
            verify(statelessCommitService, times(1)).addListenerForUploadedGeneration(
                any(),
                eq(refreshResult.generation()),
                anyActionListener()
            );
        } else {
            verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());
        }

        // Internal refresh RTG
        engine.index(randomDoc(String.valueOf(2)));
        assertTrue(engine.refreshNeeded());
        refreshResult = engine.refreshInternalSearcher(randomFrom("realtime_get", "unsafe_version_map"), true);
        if (statelessCommitService.isStatelessUploadDelayed() == false) {
            verify(statelessCommitService, times(1)).addListenerForUploadedGeneration(
                any(),
                eq(refreshResult.generation()),
                anyActionListener()
            );
        } else {
            verify(statelessCommitService, never()).addListenerForUploadedGeneration(any(), anyLong(), anyActionListener());
        }
    }

    public void testFlushesWaitForUpload() throws IOException {
        Settings nodeSettings = Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), randomBoolean())
            .build();

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                )
            )
        ) {
            final var statelessCommitService = engine.getStatelessCommitService();
            when(statelessCommitService.isStatelessUploadDelayed()).thenReturn(randomBoolean());

            engine.index(randomDoc(String.valueOf(0)));
            final PlainActionFuture<Engine.FlushResult> future = new PlainActionFuture<>();
            final boolean force = randomBoolean();
            engine.flush(force, force || randomBoolean(), future);
            final Engine.FlushResult flushResult = future.actionGet();
            verify(statelessCommitService, times(1)).addListenerForUploadedGeneration(
                any(),
                eq(flushResult.generation()),
                anyActionListener()
            );
        }
    }

    public void testStartingTranslogFileIsPlaceInUserData() throws Exception {
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();

        TranslogReplicator replicator = mock(TranslogReplicator.class);
        long maxUploadedFile = randomLongBetween(10, 20);
        when(replicator.getMaxUploadedFile()).thenReturn(maxUploadedFile, maxUploadedFile + 1);

        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60)).build(),
                    nodeSettings
                ),
                replicator
            )
        ) {
            engine.index(randomDoc(String.valueOf(0)));
            // Refresh to warm-up engine
            engine.refresh("test");
            engine.flush();
            try (Engine.IndexCommitRef indexCommitRef = engine.acquireLastIndexCommit(false)) {
                assertEquals(
                    maxUploadedFile + 1,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE))
                );
            }
        }
    }

    /**
     * Test that the engine calls the closed reader notification for every reader that it no longer uses.
     */
    public void testClosedReaders() throws IOException {
        TranslogReplicator translogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService commitService = mockCommitService(Settings.EMPTY);
        Set<PrimaryTermAndGeneration> openReaderGenerations = new HashSet<>();
        Set<Long> closedReaderGenerations = new HashSet<>();
        when(commitService.getCommitBCCResolverForShard(any())).thenReturn(commitGeneration -> {
            var commitPrimaryTermAndGeneration = new PrimaryTermAndGeneration(1, commitGeneration);
            openReaderGenerations.add(commitPrimaryTermAndGeneration);
            return Set.of(commitPrimaryTermAndGeneration);
        });
        when(commitService.getIndexEngineLocalReaderListenerForShard(any())).thenReturn((bccHoldingClosedCommit, openBCCs) -> {
            for (PrimaryTermAndGeneration primaryTermAndGeneration : openReaderGenerations) {
                if (openBCCs.contains(primaryTermAndGeneration) == false) {
                    closedReaderGenerations.add(primaryTermAndGeneration.generation());
                }
            }
            openReaderGenerations.removeIf(gen -> openBCCs.contains(gen) == false);
        });
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        try (
            var engine = newIndexEngine(
                indexConfig(
                    Settings.builder()
                        .put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(60))
                        .build(),
                    nodeSettings,
                    () -> 1L,
                    NoMergePolicy.INSTANCE
                ),
                translogReplicator,
                mock(ObjectStoreService.class),
                commitService
            )
        ) {
            final var initialGen = engine.getCurrentGeneration();
            engine.index(randomDoc(String.valueOf(0)));
            if (randomBoolean()) {
                engine.refresh("test");
            }
            engine.flush(true, true);
            assertThat(closedReaderGenerations, empty());
            // external reader manager refresh.
            engine.refresh("test");
            Set<Long> expectedClosedReaderGenerations = new HashSet<>();
            expectedClosedReaderGenerations.add(initialGen);
            assertThat(closedReaderGenerations, equalTo(expectedClosedReaderGenerations));

            final var gen = engine.getCurrentGeneration();
            int commits = between(1, 10);
            NavigableMap<Long, Engine.Searcher> searchers = new TreeMap<>();
            for (int i = 0; i < commits; ++i) {
                if (randomBoolean()) {
                    searchers.put(i + gen, engine.acquireSearcher("test"));
                }
                engine.index(randomDoc(String.valueOf(i + 1)));
                engine.flush(true, true);
                engine.refresh("test");
            }

            // closedReaderGenerations must contain [initialGen .. gen + commits) - {open searchers}
            LongStream.range(initialGen, gen + commits)
                .filter(generation -> searchers.containsKey(generation) == false)
                .forEach(expectedClosedReaderGenerations::add);
            assertThat(closedReaderGenerations, equalTo(expectedClosedReaderGenerations));

            while (searchers.isEmpty() == false) {
                long generation = randomFrom(searchers.keySet());
                Engine.Searcher searcher = searchers.remove(generation);
                searcher.close();
                expectedClosedReaderGenerations.add(generation);
                assertThat(closedReaderGenerations, equalTo(expectedClosedReaderGenerations));
            }
        }
    }

    public void testReadVirtualBatchedCompoundCommitChunkWillWorkForHugeVBCC() throws IOException {
        TranslogReplicator translogReplicator = mock(TranslogReplicator.class);
        Settings nodeSettings = Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build();
        StatelessCommitService commitService = mockCommitService(Settings.EMPTY);
        final VirtualBatchedCompoundCommit virtualBcc = mock(VirtualBatchedCompoundCommit.class);
        final long vbccSize = randomLongBetween(Long.MAX_VALUE / 2, Long.MAX_VALUE);
        when(virtualBcc.getTotalSizeInBytes()).thenReturn(vbccSize);
        try (
            var engine = newIndexEngine(
                indexConfig(Settings.EMPTY, nodeSettings, () -> 1L, NoMergePolicy.INSTANCE),
                translogReplicator,
                mock(ObjectStoreService.class),
                commitService
            )
        ) {
            final long primaryTerm = randomLongBetween(1, 42);
            final long generation = randomLongBetween(1, 9999);
            when(
                commitService.getVirtualBatchedCompoundCommit(
                    eq(engine.config().getShardId()),
                    eq(new PrimaryTermAndGeneration(primaryTerm, generation))
                )
            ).thenReturn(virtualBcc);
            final long offset = randomLongBetween(0, Long.MAX_VALUE / 2);
            final int length = randomNonNegativeInt();
            final var request = new GetVirtualBatchedCompoundCommitChunkRequest(
                engine.config().getShardId(),
                primaryTerm,
                generation,
                offset,
                length,
                "_na_"
            );
            final StreamOutput output = mock(StreamOutput.class);
            engine.readVirtualBatchedCompoundCommitChunk(request, output);

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

    public void testDocSizeIsReportedUponSuccessfulIndexAndAlwaysWhenParsingFinished() throws IOException {
        TranslogReplicator mockTranslogReplicator = mock(TranslogReplicator.class);
        StatelessCommitService mockCommitService = mockCommitService(Settings.EMPTY);
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(DocumentSizeAccumulator.EMPTY_INSTANCE);

        EngineConfig indexConfig = indexConfig();
        try (
            var engine = newIndexEngine(
                indexConfig,
                mockTranslogReplicator,
                mock(ObjectStoreService.class),
                mockCommitService,
                documentParsingProvider
            )
        ) {
            Engine.Index index = randomDoc("id");
            DocumentSizeReporter documentSizeReporter = mock(DocumentSizeReporter.class);
            when(
                documentParsingProvider.newDocumentSizeReporter(
                    eq(indexConfig.getShardId().getIndexName()),
                    any(DocumentSizeAccumulator.class)
                )
            ).thenReturn(documentSizeReporter);

            Engine.IndexResult result = engine.index(index);
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            verify(documentSizeReporter).onParsingCompleted(eq(index.parsedDoc()));
            verify(documentSizeReporter).onIndexingCompleted(eq(index.parsedDoc()));

            Engine.Index newIndex = randomDoc("id");
            var failIndex = versionConflictingIndexOperation(newIndex);
            result = engine.index(failIndex);

            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            verify(documentSizeReporter).onParsingCompleted(eq(index.parsedDoc())); // we report parsing before indexing
            verify(documentSizeReporter, times(0)).onIndexingCompleted(eq(newIndex.parsedDoc()));
        }
    }

    public void testCommitUserDataIsEnrichedByAccumulatorData() throws IOException {
        TranslogReplicator mockTranslogReplicator = mock(TranslogReplicator.class);
        when(mockTranslogReplicator.getMaxUploadedFile()).thenReturn(456L);

        StatelessCommitService mockCommitService = mockCommitService(Settings.EMPTY);
        DocumentParsingProvider documentParsingProvider = mock(DocumentParsingProvider.class);
        DocumentSizeAccumulator documentSizeAccumulator = mock(DocumentSizeAccumulator.class);
        when(documentSizeAccumulator.getAsCommitUserData(any(SegmentInfos.class))).thenReturn(Map.of("accumulator_field", "123"));
        when(documentParsingProvider.createDocumentSizeAccumulator()).thenReturn(documentSizeAccumulator);

        EngineConfig indexConfig = indexConfig();
        try (
            var engine = newIndexEngine(
                indexConfig,
                mockTranslogReplicator,
                mock(ObjectStoreService.class),
                mockCommitService,
                documentParsingProvider
            )
        ) {
            DocumentSizeReporter documentSizeReporter = mock(DocumentSizeReporter.class);
            when(documentParsingProvider.newDocumentSizeReporter(eq(indexConfig.getShardId().getIndexName()), eq(documentSizeAccumulator)))
                .thenReturn(documentSizeReporter);

            engine.index(randomDoc(String.valueOf(0)));
            engine.flush();

            try (Engine.IndexCommitRef indexCommitRef = engine.acquireLastIndexCommit(false)) {
                assertEquals(123L, Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get("accumulator_field")));
                assertEquals(
                    457L,
                    Long.parseLong(indexCommitRef.getIndexCommit().getUserData().get(IndexEngine.TRANSLOG_RECOVERY_START_FILE))
                );
            }
        }
    }

    private static Engine.Index versionConflictingIndexOperation(Engine.Index indexOp) throws IOException {
        return new Engine.Index(
            newUid(indexOp.parsedDoc()),
            indexOp.parsedDoc(),
            UNASSIGNED_SEQ_NO,
            1,
            Versions.MATCH_DELETED,
            VersionType.INTERNAL,
            PRIMARY,
            0,
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0
        );
    }
}
