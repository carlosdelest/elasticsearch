/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class StatelessPersistedClusterStateService extends PersistedClusterStateService {
    private static final String CLUSTER_STATE_STAGING_DIR_NAME = "_tmp_state";
    private final ThreadPool threadPool;
    private final NodeEnvironment nodeEnvironment;
    private final ClusterSettings clusterSettings;
    private final SetOnce<LongSupplier> currentTermSupplier = new SetOnce<>();
    private final Supplier<ObjectStoreService> objectStoreServiceSupplier;

    public StatelessPersistedClusterStateService(
        NodeEnvironment nodeEnvironment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeMillisSupplier,
        Supplier<ObjectStoreService> objectStoreServiceSupplier,
        ThreadPool threadPool
    ) {
        super(nodeEnvironment, namedXContentRegistry, clusterSettings, relativeTimeMillisSupplier);
        this.objectStoreServiceSupplier = objectStoreServiceSupplier;
        this.threadPool = threadPool;
        this.nodeEnvironment = nodeEnvironment;
        this.clusterSettings = clusterSettings;
    }

    @Override
    protected Directory createDirectory(Path path) throws IOException {
        return new BlobStoreSyncDirectory(
            super.createDirectory(path),
            this::getBlobContainerForCurrentTerm,
            threadPool.executor(getUploadsThreadPool())
        );
    }

    protected String getUploadsThreadPool() {
        return ThreadPool.Names.SNAPSHOT;
    }

    protected String getDownloadsThreadPool() {
        return ThreadPool.Names.SNAPSHOT;
    }

    private BlobContainer getBlobContainerForCurrentTerm() {
        assert currentTermSupplier.get() != null;
        return objectStoreService().getClusterStateBlobContainerForTerm(currentTermSupplier.get().getAsLong());
    }

    public CoordinationState.PersistedState createPersistedState(Settings settings, DiscoveryNode localNode) throws IOException {
        var persistedState = new StatelessPersistedState(
            this,
            objectStoreService()::getClusterStateBlobContainerForTerm,
            threadPool.executor(getDownloadsThreadPool()),
            getStateStagingPath(),
            getInitialState(settings, localNode, clusterSettings)
        );
        currentTermSupplier.set(persistedState::getCurrentTerm);
        return persistedState;
    }

    Path getStateStagingPath() {
        var dataPath = nodeEnvironment.nodeDataPaths()[0];
        return dataPath.resolve(CLUSTER_STATE_STAGING_DIR_NAME);
    }

    private ObjectStoreService objectStoreService() {
        return Objects.requireNonNull(objectStoreServiceSupplier.get());
    }

    private static ClusterState getInitialState(Settings settings, DiscoveryNode localNode, ClusterSettings clusterSettings) {
        return Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, localNode, TransportVersion.CURRENT))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterSettings))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .andThen(state -> addLocalNodeVotingConfig(state, localNode))
            .andThen(StatelessPersistedClusterStateService::withClusterUUID)
            .apply(ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings)).build());
    }

    private static ClusterState addLocalNodeVotingConfig(ClusterState clusterState, DiscoveryNode localNode) {
        var localNodeVotingConfiguration = CoordinationMetadata.VotingConfiguration.of(localNode);
        return ClusterState.builder(clusterState)
            .metadata(
                clusterState.metadata()
                    .withCoordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastCommittedConfiguration(localNodeVotingConfiguration)
                            .lastAcceptedConfiguration(localNodeVotingConfiguration)
                            .build()
                    )
            )
            .build();
    }

    private static ClusterState withClusterUUID(ClusterState clusterState) {
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).generateClusterUuidIfNeeded().build())
            .build();
    }
}
