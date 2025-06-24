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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.BaseSettingsUpdater;
import org.elasticsearch.common.settings.ProjectScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;

import java.util.Objects;

public final class ProjectSettingsUpdater extends BaseSettingsUpdater {
    public ProjectSettingsUpdater(ProjectScopedSettings scopedSettings) {
        super(scopedSettings);
    }

    public Settings updateProjectSettings(
        final ProjectId projectId,
        final Settings existingSettings,
        final Settings settingsToApply,
        final Logger logger
    ) {
        final String settingsType = "project[" + projectId + "]";

        final Settings finalSettings;
        final Settings unknownOrInvalidSettings;
        if (existingSettings.isEmpty()) {
            // Update the project settings for the first time, i.e., either as part of the project creation or right after it
            Settings.Builder builder = Settings.builder();
            scopedSettings.updateSettings(settingsToApply, builder, Settings.builder(), settingsType);
            finalSettings = builder.build();
            unknownOrInvalidSettings = Settings.EMPTY;
            logger.debug("Project [{}] has empty settings, update it to [{}]", projectId, finalSettings);
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
                return existingSettings;
            }
            finalSettings = builder.build();
            logger.debug("Update project [{}] with settings [{}]", projectId, dynamicSettingsToApply);
        }
        // validate that settings and their values are correct
        scopedSettings.validate(finalSettings, true);

        Settings resultSettings = Settings.builder().put(finalSettings).put(unknownOrInvalidSettings).build();
        // validate that SettingsUpdaters can be applied without errors
        scopedSettings.validateUpdate(resultSettings);

        return resultSettings;
    }
}
