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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TransportGetProjectStatusAction extends TransportMasterNodeReadAction<
    TransportGetProjectStatusAction.Request,
    TransportGetProjectStatusAction.Response> {

    public static final String NAME = "cluster:admin/serverless/project_status";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    @Inject
    public TransportGetProjectStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        ActionListener.completeWith(listener, () -> {
            boolean exists = state.metadata().hasProject(ProjectId.fromId(request.projectId));
            if (exists == false) {
                throw new ResourceNotFoundException("project [" + request.projectId + "] not found");
            }
            return new Response(request.projectId);
        });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    public static class Request extends MasterNodeReadRequest<Request> {
        private final String projectId;

        public Request(TimeValue masterNodeTimeout, String projectId) {
            super(masterNodeTimeout);
            this.projectId = projectId;
        }

        protected Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(projectId);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (ProjectId.isValidFormatId(projectId) == false) {
                var validationError = new ActionRequestValidationException();
                validationError.addValidationError("Invalid project ID");
                return validationError;
            }
            return null;
        }

        public String getProjectId() {
            return projectId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Request == false) return false;
            Request other = (Request) o;
            return Objects.equals(projectId, other.projectId);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(projectId);
        }

        @Override
        public String toString() {
            return "GetProjectStatusRequest{" + "projectId='" + projectId + '\'' + '}';
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        static final ParseField PROJECT_ID = new ParseField("project_id");

        private final String projectId;

        public Response(String projectId) {
            this.projectId = projectId;
        }

        public Response(StreamInput in) throws IOException {
            projectId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(projectId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PROJECT_ID.getPreferredName(), projectId);
            return builder.endObject();
        }

        public String getProjectId() {
            return projectId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response other = (Response) o;
            return Objects.equals(projectId, other.projectId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(projectId);
        }

        @Override
        public String toString() {
            return "GetProjectStatusResponse{" + "projectId='" + projectId + '\'' + '}';
        }
    }
}
