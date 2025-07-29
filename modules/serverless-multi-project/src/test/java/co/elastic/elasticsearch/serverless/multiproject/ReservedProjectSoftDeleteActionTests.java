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

package co.elastic.elasticsearch.serverless.multiproject;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class ReservedProjectSoftDeleteActionTests extends ESTestCase {

    private TransformState processJSON(ProjectId projectId, ReservedProjectSoftDeleteAction action, TransformState prevState, String json)
        throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken();
            return action.transform(projectId, action.fromXContent(parser), prevState);
        }
    }

    private static ClusterState createClusterStateWithProjectId(ProjectId projectId) {
        ProjectStateRegistry projectStateRegistry = ProjectStateRegistry.builder()
            .putProjectSettings(projectId, Settings.builder().build())
            .build();
        return createClusterStateWithRegistry(projectId, projectStateRegistry);
    }

    private static ClusterState createClusterStateWithRegistry(ProjectId projectId, ProjectStateRegistry projectStateRegistry) {
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .putCustom(ProjectStateRegistry.TYPE, projectStateRegistry)
            .putProjectMetadata(projectMetadata)
            .build();
    }

    public void testMarkProjectForDeletion() throws Exception {
        ProjectId projectId = randomUniqueProjectId();
        ClusterState clusterState = createClusterStateWithProjectId(projectId);
        TransformState prevState = new TransformState(clusterState, Collections.emptySet());

        ReservedProjectSoftDeleteAction action = new ReservedProjectSoftDeleteAction();
        String json = "true";

        TransformState transformedState = processJSON(projectId, action, prevState, json);

        ProjectStateRegistry registry = transformedState.state().custom(ProjectStateRegistry.TYPE);
        assertNotNull(registry);
        assertThat(registry.isProjectMarkedForDeletion(projectId), is(true));

        Set<String> keys = transformedState.keys();
        assertThat(keys.size(), is(1));
        assertThat(keys, contains(ReservedProjectSoftDeleteAction.NAME));

        assertThat(transformedState.state().blocks().hasGlobalBlock(projectId, PROJECT_UNDER_DELETION_BLOCK), is(true));
    }

    public void testMarkUnknownProjectForDeletion() throws Exception {
        ProjectId projectId = randomUniqueProjectId();
        TransformState prevState = new TransformState(
            createClusterStateWithRegistry(projectId, ProjectStateRegistry.builder().build()),
            Collections.emptySet()
        );

        ReservedProjectSoftDeleteAction action = new ReservedProjectSoftDeleteAction();
        String json = "true";

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> processJSON(projectId, action, prevState, json)
        );

        assertThat(exception.getMessage(), is("Cannot mark projects for deletion that are not in the registry: [" + projectId + "]"));
    }

    public void testMarkDefaultProjectForDeletion() {
        TransformState prevState = new TransformState(
            createClusterStateWithRegistry(ProjectId.DEFAULT, ProjectStateRegistry.builder().build()),
            Collections.emptySet()
        );

        ReservedProjectSoftDeleteAction action = new ReservedProjectSoftDeleteAction();
        String json = "true";

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> processJSON(ProjectId.DEFAULT, action, prevState, json)
        );

        assertThat(exception.getMessage(), is("Default project cannot be marked for deletion"));
    }

    public void testProjectAlreadyMarkedForDeletion() throws Exception {
        ProjectId projectId = randomUniqueProjectId();

        ProjectStateRegistry projectStateRegistry = ProjectStateRegistry.builder()
            .putProjectSettings(projectId, Settings.builder().build())
            .markProjectForDeletion(projectId)
            .build();
        TransformState prevState = new TransformState(
            createClusterStateWithRegistry(projectId, projectStateRegistry),
            Collections.emptySet()
        );

        ReservedProjectSoftDeleteAction action = new ReservedProjectSoftDeleteAction();
        String json = "true";

        TransformState transformedState = processJSON(projectId, action, prevState, json);
        assertThat(transformedState.state().blocks().hasGlobalBlock(projectId, PROJECT_UNDER_DELETION_BLOCK), is(true));

        ProjectStateRegistry updatedRegistry = transformedState.state().custom(ProjectStateRegistry.TYPE);
        assertNotNull(updatedRegistry);
        assertThat(updatedRegistry.isProjectMarkedForDeletion(projectId), is(true));

        Set<String> keys = transformedState.keys();
        assertThat(keys.size(), is(1));
        assertThat(keys, contains(ReservedProjectSoftDeleteAction.NAME));
    }

    public void testMarkProjectForDeletionFalse() throws Exception {
        ProjectId projectId = randomUniqueProjectId();

        ProjectStateRegistry projectStateRegistry = ProjectStateRegistry.builder()
            .putProjectSettings(projectId, Settings.builder().build())
            .build();
        TransformState prevState = new TransformState(
            createClusterStateWithRegistry(projectId, projectStateRegistry),
            Collections.emptySet()
        );

        ReservedProjectSoftDeleteAction action = new ReservedProjectSoftDeleteAction();
        String json = "false";

        TransformState transformedState = processJSON(projectId, action, prevState, json);

        ProjectStateRegistry updatedRegistry = transformedState.state().custom(ProjectStateRegistry.TYPE);
        assertNotNull(updatedRegistry);
        assertThat(updatedRegistry.isProjectMarkedForDeletion(projectId), is(false));

        Set<String> keys = transformedState.keys();
        assertTrue(keys.isEmpty());

        assertThat(transformedState.state().blocks().hasGlobalBlock(projectId, PROJECT_UNDER_DELETION_BLOCK), is(false));
    }

    public void testMarkProjectForDeletionFalseWhileBeingDeleted() {
        ProjectId projectId = randomUniqueProjectId();

        ProjectStateRegistry projectStateRegistry = ProjectStateRegistry.builder()
            .putProjectSettings(projectId, Settings.builder().build())
            .markProjectForDeletion(projectId)
            .build();
        TransformState prevState = new TransformState(
            createClusterStateWithRegistry(projectId, projectStateRegistry),
            Collections.emptySet()
        );

        ReservedProjectSoftDeleteAction action = new ReservedProjectSoftDeleteAction();
        String json = "false";

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> processJSON(projectId, action, prevState, json)
        );

        assertThat(
            exception.getMessage(),
            is("Project [" + projectId + "] is currently being deleted, can't change 'marked_for_deletion' to false")
        );
    }
}
