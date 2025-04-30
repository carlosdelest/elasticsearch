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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ProjectScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class ReservedProjectSettingsAction implements ReservedClusterStateHandler<ProjectMetadata, Map<String, Object>> {
    private static final Logger log = LogManager.getLogger(ReservedProjectSettingsAction.class);
    public static final String NAME = "project_settings";

    private final ProjectScopedSettings projectScopedSettings;

    public ReservedProjectSettingsAction(ProjectScopedSettings projectScopedSettings) {
        this.projectScopedSettings = projectScopedSettings;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransformState<ProjectMetadata> transform(Map<String, Object> source, TransformState<ProjectMetadata> prevState) {
        Settings settingsToApply = Settings.builder().loadFromMap(source).build();

        ProjectMetadata projectMetadata = prevState.state();
        ProjectMetadata updatedMetadata = new ProjectSettingsUpdater(projectScopedSettings).updateProjectSettings(
            projectMetadata,
            settingsToApply,
            log
        );

        return new TransformState<>(updatedMetadata, updatedMetadata.settings().keySet());
    }

    @Override
    public Map<String, Object> fromXContent(XContentParser parser) throws IOException {
        return parser.map();
    }
}
