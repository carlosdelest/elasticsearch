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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.BaseSettingsUpdater;
import org.elasticsearch.common.settings.ProjectScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;

public final class ProjectSettingsUpdater extends BaseSettingsUpdater {
    public ProjectSettingsUpdater(ProjectScopedSettings scopedSettings) {
        super(scopedSettings);
    }

    public ProjectMetadata updateProjectSettings(
        final ProjectMetadata projectMetadata,
        final Settings settingsToApply,
        final Logger logger
    ) {
        String settingsType = "project[" + projectMetadata.id() + "]";
        final Tuple<Settings, Settings> partitionedSettings = partitionKnownAndValidSettings(
            projectMetadata.settings(),
            settingsType,
            logger
        );
        final Settings knownAndValidPersistentSettings = partitionedSettings.v1();
        final Settings unknownOrInvalidSettings = partitionedSettings.v2();
        Settings.Builder builder = Settings.builder().put(knownAndValidPersistentSettings);

        boolean changed = scopedSettings.updateDynamicSettings(settingsToApply, builder, Settings.builder(), settingsType);
        if (changed == false) {
            return projectMetadata;
        }

        Settings finalSettings = builder.build();
        // validate that settings and their values are correct
        scopedSettings.validate(finalSettings, true);

        Settings resultSettings = Settings.builder().put(finalSettings).put(unknownOrInvalidSettings).build();
        ProjectMetadata.Builder result = ProjectMetadata.builder(projectMetadata).settings(resultSettings);
        // validate that SettingsUpdaters can be applied without errors
        scopedSettings.validateUpdate(resultSettings);

        return result.build();
    }
}
