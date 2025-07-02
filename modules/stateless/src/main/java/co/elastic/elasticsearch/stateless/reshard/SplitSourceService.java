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

package co.elastic.elasticsearch.stateless.reshard;

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class SplitSourceService {
    private static final Logger logger = LogManager.getLogger(SplitSourceService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ObjectStoreService objectStoreService;
    private final ReshardIndexService reshardIndexService;

    private final ConcurrentHashMap<IndexShard, Split> onGoingSplits = new ConcurrentHashMap<>();

    public SplitSourceService(
        Client client,
        ClusterService clusterService,
        IndicesService indicesService,
        ObjectStoreService objectStoreService,
        ReshardIndexService reshardIndexService
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.objectStoreService = objectStoreService;
        this.reshardIndexService = reshardIndexService;
    }

    public void setupTargetShard(ShardId targetShardId, long sourcePrimaryTerm, long targetPrimaryTerm, ActionListener<Void> listener) {
        Index index = targetShardId.getIndex();

        var indexMetadata = clusterService.state().metadata().projectFor(index).getIndexSafe(index);
        var reshardingMetadata = indexMetadata.getReshardingMetadata();
        assert reshardingMetadata != null && reshardingMetadata.isSplit() : "Unexpected resharding state";
        int sourceShardIndex = reshardingMetadata.getSplit().sourceShard(targetShardId.getId());
        var sourceShardId = new ShardId(index, sourceShardIndex);

        if (reshardingMetadata.getSplit().getSourceShardState(sourceShardIndex) != IndexReshardingState.Split.SourceShardState.SOURCE) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard state should be SOURCE but it is [%s]. Failing the request.",
                sourceShardId,
                targetShardId,
                reshardingMetadata.getSplit().getSourceShardState(sourceShardIndex)
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        if (reshardingMetadata.getSplit().getTargetShardState(targetShardId.getId()) != IndexReshardingState.Split.TargetShardState.CLONE) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Target shard state should be CLONE but it is [%s]. Failing the request.",
                sourceShardId,
                targetShardId,
                reshardingMetadata.getSplit().getTargetShardState(targetShardId.getId())
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        long currentSourcePrimaryTerm = indexMetadata.primaryTerm(sourceShardId.getId());
        long currentTargetPrimaryTerm = indexMetadata.primaryTerm(targetShardId.getId());

        if (currentTargetPrimaryTerm > targetPrimaryTerm) {
            // This request is stale, we will handle it when target recovers with new primary term. Fail the target recovery process.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Target primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                targetPrimaryTerm,
                currentTargetPrimaryTerm
            );
            logger.info(message);

            throw new IllegalStateException(message);
        }
        if (currentSourcePrimaryTerm > sourcePrimaryTerm) {
            // We need to keep the invariant that target primary term is >= source primary term.
            // So if source primary term advanced we need to fail target recovery so that it picks up new primary term.
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source primary term advanced [%s -> %s] before start split request was handled. Failing the request.",
                sourceShardId,
                targetShardId,
                sourcePrimaryTerm,
                currentSourcePrimaryTerm
            );
            logger.info(message);

            throw new IllegalStateException(message);
        }

        var sourceShard = indicesService.indexServiceSafe(index).getShard(sourceShardIndex);
        // Defensive check so that some other process (like recovery) won't interfere with resharding.
        if (sourceShard.state() != IndexShardState.STARTED) {
            String message = String.format(
                Locale.ROOT,
                "Split [%s -> %s]. Source shard is not started when processing start split request. Failing the request.",
                sourceShardId,
                targetShardId
            );
            logger.error(message);

            throw new IllegalStateException(message);
        }

        if (onGoingSplits.putIfAbsent(sourceShard, new Split()) == null) {
            // This is the first time a target shard contacted this source shard to start a split.
            // We'll start tracking this split now to be able to eventually properly finish it.
            // If we have already seen this split before, we are all set already.
            setupSplitProgressTracking(sourceShard);
        }

        ActionListener.run(listener, l -> {
            objectStoreService.copyShard(sourceShardId, targetShardId, sourcePrimaryTerm);
            l.onResponse(null);
        });
    }

    public void afterSplitSourceIndexShardRecovery(
        IndexShard indexShard,
        IndexReshardingMetadata reshardingMetadata,
        ActionListener<Void> listener
    ) {
        IndexReshardingState.Split split = reshardingMetadata.getSplit();

        if (split.getSourceShardState(indexShard.shardId().getId()) == IndexReshardingState.Split.SourceShardState.DONE) {
            // Nothing to do.
            // TODO eventually we should initiate deletion of resharding metadata here if all source shards are DONE.
            // This is in case master dropped the cluster state observer that does that.
            return;
        }

        if (split.targetsDone(indexShard.shardId().getId())) {
            // TODO this is not really correct.
            // This will block source shard recovery until we delete unowned documents and transition to DONE.
            // It is not necessary and this shard is serving indexing traffic so this delay
            // hurts.
            // We should not block on this but also we need to make sure this eventually happens somehow.
            // For simplicity it is done inline now.
            moveToDone(indexShard, listener);
            return;
        }

        assert onGoingSplits.containsKey(indexShard) == false;
        onGoingSplits.put(indexShard, new Split());

        setupSplitProgressTracking(indexShard);
        listener.onResponse(null);
    }

    public void setupSplitProgressTracking(IndexShard indexShard) {
        // Wait for all target shards to be DONE and once that is true execute source shard logic to move to DONE:
        // 1. Delete unowned documents
        // 2. Move to DONE
        ClusterStateObserver observer = new ClusterStateObserver(
            clusterService,
            null,
            logger,
            clusterService.threadPool().getThreadContext()
        );

        var allTargetsAreDonePredicate = new Predicate<ClusterState>() {
            @Override
            public boolean test(ClusterState state) {
                IndexReshardingState.Split split = getSplit(state, indexShard.shardId().getIndex());
                if (split == null) {
                    // This is possible if the source shard failed right after setting up this listener,
                    // recovered and successfully completed the split.
                    // TODO we can even we see a completely different split here
                    // TODO is project deletion possible here?
                    return true;
                }

                if (onGoingSplits.containsKey(indexShard) == false) {
                    // Shard was closed in the meantime.
                    // It will pick this work up on recovery.
                    return true;
                }

                return split.targetsDone(indexShard.shardId().getId());
            }
        };

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                IndexReshardingState.Split split = getSplit(state, indexShard.shardId().getIndex());
                if (split == null) {
                    return;
                }

                if (onGoingSplits.containsKey(indexShard) == false) {
                    return;
                }

                // It is possible that shard gets closed right after this line.
                // It is not really a problem since delete of unowned documents is idempotent.
                // TODO track the result of this operation, fail shard if it fails?
                clusterService.threadPool().generic().submit(() -> moveToDone(indexShard, ActionListener.noop()));
            }

            @Override
            public void onClusterServiceClose() {
                // nothing to do
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                assert false;
            }
        }, allTargetsAreDonePredicate);
    }

    public void cancelSplits(IndexShard indexShard) {
        onGoingSplits.remove(indexShard);
    }

    private void moveToDone(IndexShard indexShard, ActionListener<Void> listener) {
        SubscribableListener.<Void>newForked(l -> reshardIndexService.deleteUnownedDocuments(indexShard.shardId(), l))
            .<Void>andThen(
                l -> client.execute(
                    TransportUpdateSplitSourceStateAction.TYPE,
                    new TransportUpdateSplitSourceStateAction.Request(
                        indexShard.shardId(),
                        IndexReshardingState.Split.SourceShardState.DONE
                    ),
                    l.map(r -> null)
                )
            )
            .andThenAccept(ignored -> onGoingSplits.remove(indexShard))
            .addListener(listener);
    }

    private static IndexReshardingState.Split getSplit(ClusterState state, Index index) {
        return state.metadata()
            .lookupProject(index)
            .flatMap(p -> Optional.ofNullable(p.index(index)))
            .flatMap(m -> Optional.ofNullable(m.getReshardingMetadata()))
            .map(IndexReshardingMetadata::getSplit)
            .orElse(null);
    }

    // Currently empty, a placeholder for any metadata needed by the source shard to track the split process.
    private record Split() {}
}
