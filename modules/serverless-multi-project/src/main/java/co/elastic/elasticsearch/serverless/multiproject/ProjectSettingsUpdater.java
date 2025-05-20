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

import java.util.Objects;

public final class ProjectSettingsUpdater extends BaseSettingsUpdater {
    public ProjectSettingsUpdater(ProjectScopedSettings scopedSettings) {
        super(scopedSettings);
    }

    public ProjectMetadata updateProjectSettings(
        final ProjectMetadata projectMetadata,
        final Settings settingsToApply,
        final Logger logger
    ) {
        final Settings existingSettings = projectMetadata.settings();
        final String settingsType = "project[" + projectMetadata.id() + "]";

        final Settings finalSettings;
        final Settings unknownOrInvalidSettings;
        if (existingSettings.isEmpty()) {
            // Update the project settings for the first time, i.e., either as part of the project creation or right after it
            Settings.Builder builder = Settings.builder();
            scopedSettings.updateSettings(settingsToApply, builder, Settings.builder(), settingsType);
            finalSettings = builder.build();
            unknownOrInvalidSettings = Settings.EMPTY;
            logger.debug("Project [{}] has empty settings, update it to [{}]", projectMetadata.id(), finalSettings);
        } else {
            final Tuple<Settings, Settings> partitionedSettings = partitionKnownAndValidSettings(existingSettings, settingsType, logger);
            final Settings knownAndValidPersistentSettings = partitionedSettings.v1();
            unknownOrInvalidSettings = partitionedSettings.v2();
            Settings.Builder builder = Settings.builder().put(knownAndValidPersistentSettings);

            // Filter out settings with no changes, they could be non-dynamic settings or dynamic settings but with no changes
            final Settings dynamicSettingsToApply = settingsToApply.filter(
                key -> settingsToApply.keySet().contains(key) // the key must present in the updated settings
                    && (settingsToApply.get(key) == null // always process deletion since the key can use wildcards
                        || Objects.equals(settingsToApply.get(key), knownAndValidPersistentSettings.get(key)) == false // different values
                    )
            );
            boolean changed = scopedSettings.updateDynamicSettings(dynamicSettingsToApply, builder, Settings.builder(), settingsType);
            if (changed == false) {
                return projectMetadata;
            }
            finalSettings = builder.build();
            logger.debug("Update project [{}] with settings [{}]", projectMetadata.id(), dynamicSettingsToApply);
        }
        // validate that settings and their values are correct
        scopedSettings.validate(finalSettings, true);

        Settings resultSettings = Settings.builder().put(finalSettings).put(unknownOrInvalidSettings).build();
        ProjectMetadata.Builder result = ProjectMetadata.builder(projectMetadata).settings(resultSettings);
        // validate that SettingsUpdaters can be applied without errors
        scopedSettings.validateUpdate(resultSettings);

        return result.build();
    }
}
