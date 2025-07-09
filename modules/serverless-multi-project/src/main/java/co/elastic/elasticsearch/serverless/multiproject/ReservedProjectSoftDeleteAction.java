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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

public class ReservedProjectSoftDeleteAction implements ReservedProjectStateHandler<Boolean> {
    public static final String NAME = "marked_for_deletion";

    @Override
    public TransformState transform(ProjectId projectId, Boolean markedForDeletion, TransformState prevState) {
        ClusterState previousClusterState = prevState.state();
        ProjectStateRegistry previousProjectStateRegistry = previousClusterState.custom(ProjectStateRegistry.TYPE);
        if (markedForDeletion == false) {
            if (previousProjectStateRegistry.isProjectMarkedForDeletion(projectId)) {
                throw new IllegalArgumentException(
                    "Project [" + projectId + "] is currently being deleted, can't change 'marked_for_deletion' to false"
                );
            } else {
                return prevState;
            }
        }
        ProjectStateRegistry updatedStateRegistry = ProjectStateRegistry.builder(previousClusterState)
            .markProjectForDeletion(projectId)
            .build();
        return new TransformState(
            ClusterState.builder(previousClusterState).putCustom(ProjectStateRegistry.TYPE, updatedStateRegistry).build(),
            Collections.singleton(NAME)
        );
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Boolean fromXContent(XContentParser parser) throws IOException {
        return parser.booleanValue();
    }
}
