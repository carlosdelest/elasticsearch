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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportRegisterCommitForRecoveryAction extends HandledTransportAction<RegisterCommitRequest, RegisterCommitResponse> {

    public static final String NAME = "internal:index/shard/recovery:register_commit_for_recovery";
    public static final ActionType<RegisterCommitResponse> TYPE = new ActionType<>(NAME);

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    @Inject
    public TransportRegisterCommitForRecoveryAction(
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService,
        ActionFilters actionFilters
    ) {
        super(
            NAME,
            false,
            transportService,
            actionFilters,
            RegisterCommitRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = clusterService.threadPool();
    }

    @Override
    protected void doExecute(Task task, RegisterCommitRequest request, ActionListener<RegisterCommitResponse> listener) {
        var state = clusterService.state();
        var observer = new ClusterStateObserver(state, clusterService, new TimeValue(60000), logger, threadPool.getThreadContext());
        // todo: check if we can collapse this with the check in `StatelessCommitService.registerCommitForUnpromotableRecovery`,
        // which waits for the cluster state version to be applied.
        if (state.version() < request.getClusterStateVersion()) {
            // Indexing shard is behind
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    registerCommit(state, request, listener);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchException("timed out"));
                }
            }, s -> s.version() >= request.getClusterStateVersion());
        } else {
            registerCommit(state, request, listener);
        }
    }

    private void registerCommit(ClusterState state, RegisterCommitRequest request, ActionListener<RegisterCommitResponse> outerListener) {
        ActionListener.run(ActionListener.assertOnce(outerListener), listener -> {
            final var shardId = request.getShardId();
            if (isSearchShardInRoutingTable(state, shardId, request.getNodeId()) == false) {
                throw new ShardNotFoundException(shardId, "search shard not found in the routing table");
            }
            final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
            final var indexShard = indexService.getShard(shardId.id());
            assert indexShard.routingEntry().isPromotableToPrimary()
                : "TransportRegisterCommitForRecoveryAction can only be executed on an indexing shard";
            final var engine = indexShard.getEngineOrNull();
            if (engine == null || engine instanceof NoOpEngine) {
                // engine is closed, but search shard should be able to continue recovery
                listener.onResponse(RegisterCommitResponse.EMPTY);
                return;
            }
            StatelessCommitService statelessCommitService;
            if (engine instanceof IndexEngine e) {
                statelessCommitService = e.getStatelessCommitService();
            } else if (engine instanceof HollowIndexEngine e) {
                statelessCommitService = e.getStatelessCommitService();
            } else {
                throw new IllegalStateException("Unable to get stateless commit service for engine " + engine);
            }
            statelessCommitService.registerCommitForUnpromotableRecovery(
                request.getBatchedCompoundCommitPrimaryTermAndGeneration(),
                request.getCompoundCommitPrimaryTermAndGeneration(),
                shardId,
                request.getNodeId(),
                state,
                listener
            );
        });
    }

    private boolean isSearchShardInRoutingTable(ClusterState state, ShardId shardId, String nodeId) {
        final ProjectMetadata projectMetadata = state.metadata().projectFor(shardId.getIndex()); // can throw IndexNotFoundException
        for (var shardRouting : state.routingTable(projectMetadata.id()).shardRoutingTable(shardId).assignedUnpromotableShards()) {
            if (shardRouting.currentNodeId().equals(nodeId)) {
                return true;
            }
        }
        return false;
    }

}
