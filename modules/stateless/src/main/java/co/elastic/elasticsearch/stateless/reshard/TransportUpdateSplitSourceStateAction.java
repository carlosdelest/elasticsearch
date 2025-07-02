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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportUpdateSplitSourceStateAction extends TransportMasterNodeAction<
    TransportUpdateSplitSourceStateAction.Request,
    ActionResponse> {
    public static final ActionType<ActionResponse> TYPE = new ActionType<>("indices:admin/reshard/split_source_state");

    private final ReshardIndexService reshardIndexService;

    @Inject
    public TransportUpdateSplitSourceStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ReshardIndexService reshardIndexService,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            transportService.getThreadPool(),
            actionFilters,
            Request::new,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.reshardIndexService = reshardIndexService;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<ActionResponse> listener)
        throws Exception {
        reshardIndexService.transitionSourceState(
            request.getShardId(),
            request.getState(),
            listener.map(ignored -> ActionResponse.Empty.INSTANCE)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        ShardId shardId = request.getShardId();
        final ProjectMetadata project = state.metadata().lookupProject(shardId.getIndex()).get();
        return state.blocks().indexBlockedException(project.id(), ClusterBlockLevel.METADATA_WRITE, shardId.getIndex().getName());
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final ShardId shardId;
        private final IndexReshardingState.Split.SourceShardState state;

        public Request(ShardId shardId, IndexReshardingState.Split.SourceShardState state) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.shardId = shardId;
            this.state = state;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.state = IndexReshardingState.Split.SourceShardState.readFrom(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            state.writeTo(out);
        }

        public ShardId getShardId() {
            return shardId;
        }

        public IndexReshardingState.Split.SourceShardState getState() {
            return state;
        }
    }
}
