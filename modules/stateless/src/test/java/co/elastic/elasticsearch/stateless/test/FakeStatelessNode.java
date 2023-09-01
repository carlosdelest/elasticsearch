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

package co.elastic.elasticsearch.stateless.test;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.utils.TransferableCloseables;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static co.elastic.elasticsearch.stateless.ObjectStoreService.BUCKET_SETTING;
import static org.elasticsearch.env.Environment.PATH_REPO_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FakeStatelessNode implements Closeable {
    public final DiscoveryNode node;
    public final Path repoPath;
    public final Settings nodeSettings;
    public final ClusterSettings clusterSettings;
    public final Environment environment;
    public final IndexMetadata indexMetadata;
    public final ShardId shardId;
    public final IndexSettings indexSettings;
    public final MockTransport transport;
    public final ClusterService clusterService;
    public final NodeClient client;
    public final ShardPath indexingShardPath;
    public final IndexDirectory indexingDirectory;
    public final Store indexingStore;
    public final ShardPath searchShardPath;
    public final Directory searchDirectory;
    public final Store searchStore;
    public final TransportService transportService;
    public final RepositoriesService repoService;
    public final ObjectStoreService objectStoreService;
    public final StatelessCommitService commitService;
    public final NodeEnvironment nodeEnvironment;
    public final ThreadPool threadPool;

    public final StatelessElectionStrategy electionStrategy;
    private final StatelessCommitCleaner commitCleaner;

    private final Closeable closeables;
    private final long primaryTerm;

    public FakeStatelessNode(
        Function<Settings, Environment> environmentSupplier,
        CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        this(environmentSupplier, nodeEnvironmentSupplier, xContentRegistry, 1);
    }

    public FakeStatelessNode(
        Function<Settings, Environment> environmentSupplier,
        CheckedFunction<Settings, NodeEnvironment, IOException> nodeEnvironmentSupplier,
        NamedXContentRegistry xContentRegistry,
        long primaryTerm
    ) throws IOException {
        this.primaryTerm = primaryTerm;
        node = DiscoveryNodeUtils.create("node", ESTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
        repoPath = LuceneTestCase.createTempDir();
        nodeSettings = Settings.builder().put(PATH_REPO_SETTING.getKey(), repoPath).put(BUCKET_SETTING.getKey(), repoPath).build();
        clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        environment = environmentSupplier.apply(nodeSettings);

        indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            )
            .build();
        shardId = new ShardId(indexMetadata.getIndex(), 0);
        indexSettings = new IndexSettings(indexMetadata, nodeSettings);
        indexingShardPath = new ShardPath(
            false,
            LuceneTestCase.createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            LuceneTestCase.createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            shardId
        );
        searchShardPath = new ShardPath(
            false,
            LuceneTestCase.createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            LuceneTestCase.createTempDir().resolve(shardId.getIndex().getUUID()).resolve("0"),
            shardId
        );

        try (var localCloseables = new TransferableCloseables()) {

            threadPool = new TestThreadPool("test");
            localCloseables.add(() -> TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));

            transport = localCloseables.add(new MockTransport());
            clusterService = localCloseables.add(ClusterServiceUtils.createClusterService(threadPool));
            client = localCloseables.add(createClient(nodeSettings, threadPool));
            nodeEnvironment = nodeEnvironmentSupplier.apply(nodeSettings);
            localCloseables.add(nodeEnvironment);
            final var sharedCacheService = new SharedBlobCacheService<FileCacheKey>(
                nodeEnvironment,
                nodeSettings,
                threadPool,
                ThreadPool.Names.GENERIC
            );
            localCloseables.add(sharedCacheService);
            indexingDirectory = localCloseables.add(
                new IndexDirectory(new FsDirectoryFactory().newDirectory(indexSettings, indexingShardPath), sharedCacheService, shardId)
            );
            indexingStore = localCloseables.add(new Store(shardId, indexSettings, indexingDirectory, new DummyShardLock(shardId)));
            searchDirectory = localCloseables.add(new SearchDirectory(sharedCacheService, searchShardPath.getShardId()));
            searchStore = localCloseables.add(new Store(shardId, indexSettings, searchDirectory, new DummyShardLock(shardId)));

            transportService = transport.createTransportService(
                nodeSettings,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> node,
                null,
                Set.of()
            );

            repoService = new RepositoriesService(
                nodeSettings,
                clusterService,
                transportService,
                Map.of(
                    FsRepository.TYPE,
                    metadata -> new FsRepository(
                        metadata,
                        environment,
                        xContentRegistry,
                        clusterService,
                        BigArrays.NON_RECYCLING_INSTANCE,
                        new RecoverySettings(nodeSettings, clusterSettings)
                    ) {
                        @Override
                        protected BlobStore createBlobStore() throws Exception {
                            final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
                            final Path locationFile = environment.resolveRepoFile(location);
                            return new FsBlobStore(bufferSize, locationFile, isReadOnly()) {
                                @Override
                                public BlobContainer blobContainer(BlobPath path) {
                                    return wrapBlobContainer(path, super.blobContainer(path));
                                }
                            };
                        }
                    }
                ),
                Map.of(),
                threadPool,
                List.of()
            );

            transportService.start();
            transportService.acceptIncomingRequests();
            localCloseables.add(transportService::stop);

            objectStoreService = new ObjectStoreService(nodeSettings, () -> repoService, threadPool, clusterService);
            objectStoreService.start();
            electionStrategy = new StatelessElectionStrategy(objectStoreService::getTermLeaseBlobContainer, threadPool);
            var consistencyService = new StatelessClusterConsistencyService(clusterService, electionStrategy);
            commitCleaner = createCommitCleaner(consistencyService, threadPool, objectStoreService);
            commitService = new StatelessCommitService(
                objectStoreService,
                () -> clusterService.localNode().getEphemeralId(),
                this::getShardRoutingTable,
                clusterService.threadPool(),
                client,
                commitCleaner
            );
            commitService.register(shardId, getPrimaryTerm());
            indexingDirectory.getSearchDirectory().setBlobContainer(term -> objectStoreService.getBlobContainer(shardId, term));

            closeables = localCloseables.transfer();
        }
    }

    protected StatelessCommitCleaner createCommitCleaner(
        StatelessClusterConsistencyService consistencyService,
        ThreadPool threadPool,
        ObjectStoreService objectStoreService
    ) {
        return new StatelessCommitCleaner(consistencyService, threadPool, objectStoreService);
    }

    protected IndexShardRoutingTable getShardRoutingTable(ShardId shardId) {
        IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.shardId()).thenReturn(shardId);
        return routingTable;
    }

    protected NodeClient createClient(Settings nodeSettings, ThreadPool threadPool) {
        return new NodeClient(nodeSettings, threadPool);
    }

    protected long getPrimaryTerm() {
        return primaryTerm;
    }

    public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
        return innerContainer;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(closeables);
    }
}
