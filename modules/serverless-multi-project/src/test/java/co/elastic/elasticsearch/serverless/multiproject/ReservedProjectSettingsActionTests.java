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
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.common.settings.ProjectScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class ReservedProjectSettingsActionTests extends ESTestCase {
    private TransformState processJSON(ProjectId projectId, ReservedProjectSettingsAction action, TransformState prevState, String json)
        throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(projectId, action.fromXContent(parser), prevState);
        }
    }

    public void testSettingSet() throws Exception {
        Setting<Integer> setting = Setting.intSetting(
            "project.setting",
            0,
            Setting.Property.Dynamic,
            Setting.Property.NodeScope,
            Setting.Property.ProjectScope
        );
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).build();
        TransformState prevState = new TransformState(
            ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadata).build(),
            Collections.emptySet()
        );
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(setting));
        ReservedProjectSettingsAction action = new ReservedProjectSettingsAction(projectScopedSettings);
        String json = """
            {
                "project.setting": "43"
            }""";

        TransformState transformedState = processJSON(projectId, action, prevState, json);
        ProjectState updatedProject = transformedState.state().projectState(projectId);
        assertThat(updatedProject.settings().keySet(), contains(setting.getKey()));
        assertThat(setting.get(updatedProject.settings()), is(43));

        Set<String> keys = transformedState.keys();
        assertThat(keys.size(), is(1));
        assertThat(keys, contains(setting.getKey()));
    }

    public void testSettingUpdate() throws Exception {
        Setting<Integer> setting = Setting.intSetting(
            "project.setting",
            0,
            Setting.Property.Dynamic,
            Setting.Property.NodeScope,
            Setting.Property.ProjectScope
        );
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).build();
        Settings existingSettings = Settings.builder().put(setting.getKey(), 42).build();
        TransformState prevState = new TransformState(
            ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(projectMetadata)
                .putCustom(
                    ProjectStateRegistry.TYPE,
                    ProjectStateRegistry.builder().putProjectSettings(projectId, existingSettings).build()
                )
                .build(),
            Collections.emptySet()
        );
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(existingSettings, Set.of(setting));

        ReservedProjectSettingsAction action = new ReservedProjectSettingsAction(projectScopedSettings);
        String json = """
            {
                "project.setting": "43"
            }""";

        TransformState transformedState = processJSON(projectId, action, prevState, json);
        ProjectState updatedProject = transformedState.state().projectState(projectId);
        assertThat(updatedProject.settings().keySet(), contains(setting.getKey()));
        assertThat(setting.get(updatedProject.settings()), is(43));

        Set<String> keys = transformedState.keys();
        assertThat(keys.size(), is(1));
        assertThat(keys, contains(setting.getKey()));
    }

    public void testUnknownSetting() {
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).build();
        TransformState prevState = new TransformState(
            ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(projectMetadata).build(),
            Collections.emptySet()
        );
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Collections.emptySet());
        ReservedProjectSettingsAction action = new ReservedProjectSettingsAction(projectScopedSettings);

        String json = """
            {
                "setting1.value": "42"
            }""";

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> processJSON(projectId, action, prevState, json)).getMessage(),
            is("project[" + projectId + "] setting [setting1.value], not recognized")
        );
    }
}
