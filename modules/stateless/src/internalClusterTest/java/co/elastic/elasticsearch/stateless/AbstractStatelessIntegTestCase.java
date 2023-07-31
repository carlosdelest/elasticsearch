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

import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.StatelessRefreshThrottlingIT;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicatorReader;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.junit.Before;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractStatelessIntegTestCase extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    public static final String SYSTEM_INDEX_NAME = ".sys-idx";

    protected void createSystemIndex(Settings indexSettings) {
        assertAcked(prepareCreate(SYSTEM_INDEX_NAME).setSettings(indexSettings));
    }

    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(SYSTEM_INDEX_NAME + "*")
                    .setDescription("Test system indices")
                    .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return StatelessRefreshThrottlingIT.SystemIndexTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A simple test plugin with test indices";
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(SystemIndexTestPlugin.class, BlobCachePlugin.class, Stateless.class, MockTransportService.TestPlugin.class);
    }

    private boolean useBasePath;

    @Before
    public void setup() {
        useBasePath = randomBoolean();
    }

    protected String getFsRepoSanitizedBucketName() {
        return getTestName().replaceAll("[^0-9a-zA-Z-_]", "_") + "_bucket";
    }

    protected Settings.Builder nodeSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), getFsRepoSanitizedBucketName());
        if (useBasePath) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "base_path");
        }
        return builder;
    }

    protected String startIndexNode() {
        return startIndexNode(Settings.EMPTY);
    }

    protected String startIndexNode(Settings extraSettings) {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.INDEX_ROLE).put(extraSettings));
    }

    protected String startSearchNode() {
        return startSearchNode(Settings.EMPTY);
    }

    protected String startSearchNode(Settings extraSettings) {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.SEARCH_ROLE).put(extraSettings));
    }

    private Settings.Builder settingsForRoles(DiscoveryNodeRole... roles) {
        return nodeSettings().putList(
            NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
            Arrays.stream(roles).map(DiscoveryNodeRole::roleName).toList()
        )
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                rarely()
                    ? randomBoolean()
                        ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.KB).getStringRep()
                        : new ByteSizeValue(randomIntBetween(1, 1000), ByteSizeUnit.BYTES).getStringRep()
                    : randomBoolean() ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB).getStringRep()
                    // only use up to 0.1% disk to be friendly.
                    : new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString()
            );
    }

    protected String startMasterOnlyNode() {
        return startMasterOnlyNode(Settings.EMPTY);
    }

    protected String startMasterOnlyNode(Settings extraSettings) {
        return internalCluster().startMasterOnlyNode(nodeSettings().put(extraSettings).build());
    }

    protected String startMasterAndIndexNode() {
        return internalCluster().startNode(settingsForRoles(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE));
    }

    protected List<String> startIndexNodes(int numOfNodes) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startIndexNode());
        }
        return List.copyOf(nodes);
    }

    protected List<String> startSearchNodes(int numOfNodes) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startSearchNode());
        }
        return List.copyOf(nodes);
    }

    protected static void indexDocs(String indexName, int numDocs) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        assertNoFailures(bulkRequest.get());
    }

    protected void indexDocsAndRefresh(String indexName, int numDocs) throws Exception {
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
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false;
    }

    protected static void indexDocuments(String indexName) {
        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush(indexName).setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh(indexName).get();
                case 2 -> client().admin().indices().prepareForceMerge(indexName).get();
            }
            assertObjectStoreConsistentWithIndexShards();
        }
    }

    protected static void assertObjectStoreConsistentWithIndexShards() {
        assertObjectStoreConsistentWithShards(DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
    }

    protected static void assertObjectStoreConsistentWithSearchShards() {
        assertObjectStoreConsistentWithShards(DiscoveryNodeRole.SEARCH_ROLE, ShardRouting.Role.SEARCH_ONLY);
    }

    private static void assertObjectStoreConsistentWithShards(DiscoveryNodeRole nodeRole, ShardRouting.Role shardRole) {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                assertThatObjectStoreIsConsistentWithLastCommit(findShard(entry.getKey(), shardId, nodeRole, shardRole));
            }
        }
    }

    protected static void assertThatObjectStoreIsConsistentWithLastCommit(final IndexShard indexShard) {
        final Store store = indexShard.store();
        store.incRef();
        try {
            ObjectStoreService objectStoreService = internalCluster().getDataNodeInstance(ObjectStoreService.class);
            var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());

            // can take some time for files to be uploaded to the object store
            assertBusy(() -> {
                String commitFile = StatelessCompoundCommit.NAME + segmentInfos.getGeneration();
                assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
                StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
                    new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
                    blobContainerForCommit.listBlobs().get(commitFile).length()
                );
                var localFiles = segmentInfos.files(false);
                var expectedBlobFile = localFiles.stream().map(s -> commit.commitFiles().get(s).blobName()).collect(Collectors.toSet());
                var remoteFiles = blobContainerForCommit.listBlobs().keySet();
                assertThat(
                    "Expected that all local files " + localFiles + " exist in remote " + remoteFiles,
                    remoteFiles,
                    hasItems(expectedBlobFile.toArray(String[]::new))
                );
                for (String localFile : segmentInfos.files(false)) {
                    BlobLocation blobLocation = commit.commitFiles().get(localFile);
                    final BlobContainer blobContainerForFile = objectStoreService.getBlobContainer(
                        indexShard.shardId(),
                        blobLocation.primaryTerm()
                    );
                    assertThat(localFile, blobContainerForFile.blobExists(blobLocation.blobName()), is(true));
                    try (
                        IndexInput input = store.directory().openInput(localFile, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        InputStream remote = blobContainerForFile.readBlob(
                            blobLocation.blobName(),
                            blobLocation.offset(),
                            blobLocation.fileLength()
                        );
                    ) {
                        assertEquals("File [" + blobLocation + "] in object store has a different content than local file ", local, remote);
                    }
                }
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            store.decRef();
        }
    }

    protected static void assertThatSearchShardIsConsistentWithLastCommit(final IndexShard indexShard, final IndexShard searchShard) {
        final Store indexStore = indexShard.store();
        final Store searchStore = searchShard.store();
        indexStore.incRef();
        searchStore.incRef();
        try {
            ObjectStoreService objectStoreService = internalCluster().getDataNodeInstance(ObjectStoreService.class);
            var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(indexStore.directory());

            String commitFile = StatelessCompoundCommit.NAME + segmentInfos.getGeneration();
            assertBusy(() -> assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true)));
            StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
                new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
                blobContainerForCommit.listBlobs().get(commitFile).length()
            );

            for (String localFile : segmentInfos.files(false)) {
                var blobPath = commit.commitFiles().get(localFile);
                BlobContainer blobContainer = objectStoreService.getBlobContainer(indexShard.shardId(), blobPath.primaryTerm());
                var blobFile = blobPath.blobName();
                // can take some time for files to be uploaded to the object store
                assertBusy(() -> {
                    assertThat(blobFile, blobContainer.blobExists(blobFile), is(true));

                    try (
                        IndexInput input = indexStore.directory().openInput(localFile, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        IndexInput searchInput = searchStore.directory().openInput(localFile, IOContext.READONCE);
                        InputStream searchInputStream = new InputStreamIndexInput(searchInput, searchInput.length());
                    ) {
                        assertEquals(
                            "File [" + blobFile + "] on search shard has a different content than local file ",
                            local,
                            searchInputStream
                        );
                    }
                });
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            indexStore.decRef();
            searchStore.decRef();
        }
    }

    private static void assertEquals(String message, InputStream expected, InputStream actual) throws IOException {
        // adapted from Files.mismatch()
        final int BUFFER_SIZE = 8192;
        byte[] buffer1 = new byte[BUFFER_SIZE];
        byte[] buffer2 = new byte[BUFFER_SIZE];
        try (
            InputStream expectedStream = new BufferedInputStream(expected, BUFFER_SIZE);
            InputStream actualStream = new BufferedInputStream(actual, BUFFER_SIZE)
        ) {
            long totalRead = 0;
            while (true) {
                int nRead1 = expectedStream.readNBytes(buffer1, 0, BUFFER_SIZE);
                int nRead2 = actualStream.readNBytes(buffer2, 0, BUFFER_SIZE);

                int i = Arrays.mismatch(buffer1, 0, nRead1, buffer2, 0, nRead2);
                assertThat(message + "(position: " + (totalRead + i) + ')', i, equalTo(-1));
                if (nRead1 < BUFFER_SIZE) {
                    // we've reached the end of the files, but found no mismatch
                    break;
                }
                totalRead += nRead1;
            }
        }
    }

    protected static DiscoveryNode findIndexNode(Index index, int shardId) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.INDEX_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    if (shardOrNull != null && shardOrNull.isActive()) {
                        assertTrue(shardOrNull.routingEntry().primary());
                        return indicesService.clusterService().localNode();
                    }
                }
            }
        }
        throw new AssertionError("Cannot finding indexing node for: " + shardId);
    }

    protected static IndexShard findIndexShard(Index index, int shardId) {
        return findShard(index, shardId, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
    }

    protected static IndexShard findSearchShard(Index index, int shardId) {
        return findShard(index, shardId, DiscoveryNodeRole.SEARCH_ROLE, ShardRouting.Role.SEARCH_ONLY);
    }

    protected static IndexShard findShard(Index index, int shardId, DiscoveryNodeRole nodeRole, ShardRouting.Role shardRole) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), nodeRole)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shard = indexService.getShardOrNull(shardId);
                    if (shard != null && shard.isActive()) {
                        assertThat("Unexpected shard role", shard.routingEntry().role(), equalTo(shardRole));
                        return shard;
                    }
                }
            }
        }
        throw new AssertionError(
            "IndexShard instance not found for shard " + new ShardId(index, shardId) + " on nodes with [" + nodeRole.roleName() + "] role"
        );
    }

    protected static Map<Index, Integer> resolveIndices() {
        return client().admin()
            .indices()
            .prepareGetIndex()
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            .get()
            .getSettings()
            .values()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    settings -> new Index(
                        settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
                        settings.get(IndexMetadata.SETTING_INDEX_UUID)
                    ),
                    settings -> settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)
                )
            );
    }

    protected static void assertReplicatedTranslogConsistentWithShards() throws Exception {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                DiscoveryNode indexNode = findIndexNode(entry.getKey(), shardId);
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                final ShardId objShardId = new ShardId(entry.getKey(), shardId);

                // Check that the translog on the object store contains the correct sequence numbers and number of operations
                var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode.getName());
                var reader = new TranslogReplicatorReader(indexObjectStoreService.getTranslogBlobContainer(), objShardId);
                long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                long totalOps = 0;
                Translog.Operation next = reader.next();
                while (next != null) {
                    maxSeqNo = SequenceNumbers.max(maxSeqNo, next.seqNo());
                    totalOps++;
                    next = reader.next();
                }
                assertThat(maxSeqNo, equalTo(indexShard.seqNoStats().getMaxSeqNo()));
                assertThat(totalOps, equalTo(indexShard.seqNoStats().getMaxSeqNo() + 1));
            }
        }
    }
}
