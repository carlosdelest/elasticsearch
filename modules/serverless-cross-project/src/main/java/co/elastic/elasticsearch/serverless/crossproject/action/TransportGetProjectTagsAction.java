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

package co.elastic.elasticsearch.serverless.crossproject.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportGetProjectTagsAction extends HandledTransportAction<
    TransportGetProjectTagsAction.Request,
    TransportGetProjectTagsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetProjectTagsAction.class);

    public static final String NAME = "cluster:monitor/serverless/project_tags";  // MP TODO: cluster:monitor?
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private final RemoteClusterService remoteClusterService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final Environment env;

    @Inject
    public TransportGetProjectTagsAction(
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ActionFilters actionFilters,
        Environment env
    ) {
        super(NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.env = env;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        // Verifies we are executing on a search node.
        remoteClusterService.ensureClientIsEnabled();

        ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(clusterService.state());

        // TODO: example of how to grab local project-id once we need it in a non-stubbed out version of this action
        // ProjectId localProjectId;
        // if (projectResolver.supportsMultipleProjects()) {
        // localProjectId = projectMetadata.id();
        // } else {
        // localProjectId = ProjectId.fromId(ServerlessSharedSettings.PROJECT_ID.get(env.settings()));
        // }

        ImmutableOpenMap<String, Metadata.ProjectCustom> customs = projectMetadata.customs();
        // TODO: in later PR, figure out what new customs will be stored for project metadata tags and link info
        // current customs: [index-graveyard, ingest, component_template, model_registry, persistent_tasks, index_template]

        // hardcoding these for now - would be looked up from ProjectMetadata.customs()
        ProjectMetadataTags local = new ProjectMetadataTags("a1b2c3d45f6", "my-local-project", "elasticsearch");
        List<ProjectMetadataTags> linkedProjects = List.of(
            new ProjectMetadataTags("1111111111111", "remote1", "security"),
            new ProjectMetadataTags("2222222222222", "remote2", "observability")
        );

        listener.onResponse(new Response(local, linkedProjects));
    }

    public static class Request extends ActionRequest {

        public Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class ProjectMetadataTags implements Writeable {

        private final String projectId;
        private final String projectAlias;
        private final String projectType;

        public ProjectMetadataTags(String projectId, String projectAlias, String projectType) {
            this.projectId = Objects.requireNonNull(projectId);
            this.projectAlias = Objects.requireNonNull(projectAlias);
            this.projectType = Objects.requireNonNull(projectType);
        }

        public ProjectMetadataTags(StreamInput in) throws IOException {
            this.projectId = in.readString();
            this.projectAlias = in.readString();
            this.projectType = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(projectId);
            out.writeString(projectAlias);
            out.writeString(projectType);
        }

        public String getProjectId() {
            return projectId;
        }

        public String getProjectAlias() {
            return projectAlias;
        }

        public String getProjectType() {
            return projectType;
        }
    }

    public final class Response extends ActionResponse implements ToXContentObject {
        private ProjectMetadataTags local;
        private List<ProjectMetadataTags> linkedProjects;

        public Response(ProjectMetadataTags local, List<ProjectMetadataTags> linkedProjects) {
            this.local = local;
            this.linkedProjects = linkedProjects;
        }

        public Response(StreamInput in) throws IOException {
            this.local = new ProjectMetadataTags(in);
            this.linkedProjects = in.readOptionalCollectionAsList(ProjectMetadataTags::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            local.writeTo(out);
            out.writeOptionalCollection(linkedProjects);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject(local.getProjectId());
            {
                builder.field("alias", local.getProjectAlias());
                builder.field("type", local.getProjectType());
            }
            if (linkedProjects != null && linkedProjects.size() > 0) {
                builder.startObject("linked-projects");
                {
                    for (ProjectMetadataTags tags : linkedProjects) {
                        builder.startObject(tags.getProjectId());
                        {
                            builder.field("alias", tags.getProjectAlias());
                            builder.field("type", tags.getProjectType());
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response other = (Response) o;
            return Objects.equals(local, other.local) && Objects.equals(linkedProjects, other.linkedProjects);
        }

        @Override
        public int hashCode() {
            return Objects.hash(local, linkedProjects);
        }

        @Override
        public String toString() {
            return "GetProjectTagsResponse{" + "local=" + local + ", linkedProjects=" + linkedProjects + '}';
        }
    }
}
