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

package co.elastic.elasticsearch.serverless.multiproject.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

@FixForMultiProject(description = "This action should not exist - we should rely on file based configuration")
public class DeleteProjectAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteProjectAction INSTANCE = new DeleteProjectAction();
    public static final String NAME = "cluster:admin/projects/delete";

    private static final Logger logger = LogManager.getLogger(DeleteProjectAction.class);

    public DeleteProjectAction() {
        super(NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static class TransportDeleteProjectAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        private final MasterServiceTaskQueue<DeleteProjectTask> queue;

        @Inject
        public TransportDeleteProjectAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters
        ) {
            super(
                INSTANCE.name(),
                false,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                AcknowledgedResponse::readFrom,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.queue = clusterService.createTaskQueue("delete-project", Priority.NORMAL, new DeleteProjectExecutor());
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
            throws Exception {
            queue.submitTask("delete-project " + request.projectId, new DeleteProjectTask(request, listener), request.masterNodeTimeout());
        }
    }

    record DeleteProjectTask(Request request, ActionListener<AcknowledgedResponse> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class DeleteProjectExecutor implements ClusterStateTaskExecutor<DeleteProjectTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<DeleteProjectTask> batchExecutionContext) throws Exception {
            var metadataBuilder = Metadata.builder(batchExecutionContext.initialState().metadata());
            var routingTableBuilder = GlobalRoutingTable.builder(batchExecutionContext.initialState().globalRoutingTable());
            for (TaskContext<DeleteProjectTask> taskContext : batchExecutionContext.taskContexts()) {
                try {
                    ProjectId projectId = taskContext.getTask().request().projectId;
                    if (metadataBuilder.getProject(projectId) == null) {
                        taskContext.onFailure(new IllegalArgumentException("project [" + projectId + "] does not exist"));
                        continue;
                    }
                    metadataBuilder.removeProject(projectId);
                    routingTableBuilder.removeProject(projectId);
                    logger.info(
                        "Deleted project ["
                            + projectId
                            + "] from cluster state version ["
                            + batchExecutionContext.initialState().version()
                            + "]"
                    );
                    taskContext.success(() -> taskContext.getTask().listener.onResponse(AcknowledgedResponse.TRUE));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return ClusterState.builder(batchExecutionContext.initialState())
                .metadata(metadataBuilder.build())
                .routingTable(routingTableBuilder.build())
                .build();
        }
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final ProjectId projectId;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, ProjectId projectId) {
            super(masterNodeTimeout, ackTimeout);
            this.projectId = projectId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = new ProjectId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (projectId == null || projectId.id() == null || projectId.id().isEmpty()) {
                validationException = ValidateActions.addValidationError("project id is missing", validationException);
            }
            return validationException;
        }
    }
}
