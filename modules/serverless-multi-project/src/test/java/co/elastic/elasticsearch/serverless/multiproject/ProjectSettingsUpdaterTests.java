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
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ProjectSettingsUpdaterTests extends ESTestCase {

    private static final Setting<Float> SETTING_A = Setting.floatSetting(
        "project.setting_a",
        0.55f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );
    private static final Setting<Float> SETTING_B = Setting.floatSetting(
        "project.setting_b",
        0.1f,
        0.0f,
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );

    private static ProjectState projectWithSettings(Settings settings) {
        ProjectId projectId = randomUniqueProjectId();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putCustom(ProjectStateRegistry.TYPE, ProjectStateRegistry.builder().putProjectSettings(projectId, settings).build())
            .putProjectMetadata(ProjectMetadata.builder(projectId).build())
            .build();
        return clusterState.projectState(projectId);
    }

    public void testUpdateSetting() {
        AtomicReference<Float> valueA = new AtomicReference<>();
        AtomicReference<Float> valueB = new AtomicReference<>();
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(SETTING_A, SETTING_B));
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_A, valueA::set);
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_B, valueB::set);
        ProjectSettingsUpdater updater = new ProjectSettingsUpdater(projectScopedSettings);
        ProjectState projectState = projectWithSettings(
            Settings.builder().put(SETTING_A.getKey(), 1.5).put(SETTING_B.getKey(), 2.5).build()
        );
        Settings newSettings = updater.updateProjectSettings(
            projectState.projectId(),
            projectState.settings(),
            Settings.builder().put(SETTING_A.getKey(), 0.5).build(),
            logger
        );
        assertNotSame(newSettings, projectState.settings());
        assertEquals(SETTING_A.get(newSettings), 0.4, 0.1);
        assertEquals(SETTING_B.get(newSettings), 2.5, 0.1);

        newSettings = updater.updateProjectSettings(
            projectState.projectId(),
            projectState.settings(),
            Settings.builder().putNull("project.*").build(),
            logger
        );
        assertEquals(SETTING_A.get(newSettings), 0.55, 0.1);
        assertEquals(SETTING_B.get(newSettings), 0.1, 0.1);

        assertNull("updater only does a dryRun", valueA.get());
        assertNull("updater only does a dryRun", valueB.get());
    }

    public void testUpdateStaticSettingWorksOnlyForEmptySettings() {
        Setting<Integer> staticSetting = Setting.intSetting("project.static_setting", 0, Property.NodeScope, Property.ProjectScope);
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(
            Settings.EMPTY,
            Set.of(staticSetting, SETTING_A, SETTING_B)
        );
        ProjectSettingsUpdater updater = new ProjectSettingsUpdater(projectScopedSettings);
        // A project is created first with empty settings
        final ProjectState projectState = projectWithSettings(Settings.EMPTY);

        // Update the static setting for works for empty settings
        final Settings newSettings1 = updater.updateProjectSettings(
            projectState.projectId(),
            projectState.settings(),
            Settings.builder().put(staticSetting.getKey(), 42).put(SETTING_A.getKey(), 0.9).build(),
            logger
        );
        assertNotSame(newSettings1, projectState);
        assertEquals(42, (int) staticSetting.get(newSettings1));
        assertEquals(SETTING_A.get(newSettings1), 0.9, 0.1);
        assertEquals(SETTING_B.get(newSettings1), 0.1, 0.1);

        // Update dynamic settings works as long as the static setting is not changed
        final Settings newSettings2 = updater.updateProjectSettings(
            projectState.projectId(),
            newSettings1,
            Settings.builder().put(staticSetting.getKey(), 42).putNull(SETTING_A.getKey()).put(SETTING_B.getKey(), 0.5).build(),
            logger
        );
        assertEquals(42, (int) staticSetting.get(newSettings2));
        assertEquals(SETTING_A.get(newSettings2), 0.55, 0.1);
        assertEquals(SETTING_B.get(newSettings2), 0.5, 0.1);

        // Update the static setting errors out when the project already has settings
        Settings settingsToApply = Settings.builder().put(staticSetting.getKey(), 43).put(SETTING_A.getKey(), 0.9).build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateProjectSettings(projectState.projectId(), newSettings2, settingsToApply, logger)
        );
        assertThat(
            exception.getMessage(),
            is("project[" + projectState.projectId() + "] setting [project.static_setting], not dynamically updateable")
        );
    }

    public void testAllOrNothing() {
        AtomicReference<Float> valueA = new AtomicReference<>();
        AtomicReference<Float> valueB = new AtomicReference<>();
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(SETTING_A, SETTING_B));
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_A, valueA::set);
        projectScopedSettings.addSettingsUpdateConsumer(SETTING_B, valueB::set);
        ProjectSettingsUpdater updater = new ProjectSettingsUpdater(projectScopedSettings);
        ProjectState projectState = projectWithSettings(
            Settings.builder().put(SETTING_A.getKey(), 1.5).put(SETTING_B.getKey(), 2.5).build()
        );

        try {
            updater.updateProjectSettings(
                projectState.projectId(),
                projectState.settings(),
                Settings.builder().put(SETTING_A.getKey(), "not a float").put(SETTING_B.getKey(), 1.0f).build(),
                logger
            );
            fail("all or nothing");
        } catch (IllegalArgumentException ex) {
            logger.info("", ex);
            assertEquals("Failed to parse value [not a float] for setting [project.setting_a]", ex.getMessage());
        }
        assertNull("updater only does a dryRun", valueA.get());
        assertNull("updater only does a dryRun", valueB.get());
    }

    public void testDeprecationLogging() {
        Setting<String> deprecatedSetting = Setting.simpleString(
            "deprecated.setting",
            Property.Dynamic,
            Property.NodeScope,
            Property.ProjectScope,
            Property.DeprecatedWarning
        );
        final Settings settings = Settings.builder().put("deprecated.setting", "foo").build();
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(deprecatedSetting, SETTING_A));
        projectScopedSettings.addSettingsUpdateConsumer(deprecatedSetting, s -> {});
        ProjectSettingsUpdater settingsUpdater = new ProjectSettingsUpdater(projectScopedSettings);
        ProjectState projectState = projectWithSettings(settings);

        final Settings toApplyDebug = Settings.builder().put(SETTING_A.getKey(), 1.0f).build();
        ProjectId projectId = projectState.projectId();
        final Settings afterDebug = settingsUpdater.updateProjectSettings(projectId, projectState.settings(), toApplyDebug, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        final Settings toApplyUnset = Settings.builder().putNull(SETTING_A.getKey()).build();
        final Settings afterUnset = settingsUpdater.updateProjectSettings(projectId, afterDebug, toApplyUnset, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        // we also check that if no settings are changed, deprecation logging still occurs
        settingsUpdater.updateProjectSettings(projectId, afterUnset, toApplyUnset, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });
    }

    public void testUpdateWithUnknownAndSettings() {
        // we will randomly apply some new dynamic persistent and transient settings
        final int numberOfDynamicSettings = randomIntBetween(1, 8);
        final List<Setting<String>> dynamicSettings = new ArrayList<>(numberOfDynamicSettings);
        for (int i = 0; i < numberOfDynamicSettings; i++) {
            final Setting<String> dynamicSetting = Setting.simpleString(
                "dynamic.setting" + i,
                Property.Dynamic,
                Property.NodeScope,
                Property.ProjectScope
            );
            dynamicSettings.add(dynamicSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingSettings = Settings.builder();

        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (randomBoolean()) {
                existingSettings.put(dynamicSetting.getKey(), "existing_value");
            }
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            existingSettings.put(invalidSetting.getKey(), "value");
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            existingSettings.put(unknownSetting.getKey(), "value");
        }

        // register all the known settings (note that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            Stream.of(SETTING_A, SETTING_B),
            Stream.concat(dynamicSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, knownSettings);
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            projectScopedSettings.addSettingsUpdateConsumer(dynamicSetting, s -> {});
        }
        ProjectSettingsUpdater settingsUpdater = new ProjectSettingsUpdater(projectScopedSettings);
        ProjectState projectState = projectWithSettings(existingSettings.build());

        // prepare the dynamic settings update
        final Settings.Builder toApply = Settings.builder();
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (randomBoolean()) {
                toApply.put(dynamicSetting.getKey(), "new_value");
            }
        }
        if (toApply.keys().isEmpty()) {
            toApply.put(dynamicSettings.getFirst().getKey(), "new_value");
        }

        final Settings afterUpdate = settingsUpdater.updateProjectSettings(
            projectState.projectId(),
            projectState.settings(),
            toApply.build(),
            logger
        );

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            assertThat(afterUpdate.keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey()));
            assertThat(afterUpdate.keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            assertThat(afterUpdate.keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey()));
            assertThat(afterUpdate.keySet(), not(hasItem(unknownSetting.getKey())));
        }

        // the dynamic settings should be applied
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (toApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(afterUpdate.keySet(), hasItem(dynamicSetting.getKey()));
                assertThat(afterUpdate.get(dynamicSetting.getKey()), equalTo("new_value"));
            } else {
                if (existingSettings.keys().contains(dynamicSetting.getKey())) {
                    assertThat(afterUpdate.keySet(), hasItem(dynamicSetting.getKey()));
                    assertThat(afterUpdate.get(dynamicSetting.getKey()), equalTo("existing_value"));
                } else {
                    assertThat(afterUpdate.keySet(), not(hasItem(dynamicSetting.getKey())));
                }
            }
        }
    }

    public void testRemovingArchivedSettingsDoesNotRemoveNonArchivedInvalidOrUnknownSettings() {
        // these are settings that are archived in the cluster state as either persistent or transient settings
        final int numberOfArchivedSettings = randomIntBetween(1, 8);
        final List<Setting<String>> archivedSettings = new ArrayList<>(numberOfArchivedSettings);
        for (int i = 0; i < numberOfArchivedSettings; i++) {
            final Setting<String> archivedSetting = Setting.simpleString("setting", Property.NodeScope, Property.ProjectScope);
            archivedSettings.add(archivedSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingSettings = Settings.builder();

        for (final Setting<String> archivedSetting : archivedSettings) {
            existingSettings.put(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey(), "value");
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            existingSettings.put(invalidSetting.getKey(), "value");
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            existingSettings.put(unknownSetting.getKey(), "value");
        }

        // register all the known settings (not that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            Stream.of(SETTING_A, SETTING_B),
            Stream.concat(archivedSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, knownSettings);
        ProjectSettingsUpdater settingsUpdater = new ProjectSettingsUpdater(projectScopedSettings);
        final ProjectState projectState = projectWithSettings(existingSettings.build());

        final Settings.Builder toApply = Settings.builder().put("archived.*", (String) null);

        final Settings afterUpdate = settingsUpdater.updateProjectSettings(
            projectState.projectId(),
            projectState.settings(),
            toApply.build(),
            logger
        );

        // existing archived settings are removed
        for (final Setting<String> archivedSetting : archivedSettings) {
            assertThat(afterUpdate.keySet(), not(hasItem(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey())));
        }

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            assertThat(afterUpdate.keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey()));
            assertThat(afterUpdate.keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            assertThat(afterUpdate.keySet(), hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey()));
            assertThat(afterUpdate.keySet(), not(hasItem(unknownSetting.getKey())));
        }
    }

    private static List<Setting<String>> unknownSettings(int numberOfUnknownSettings) {
        final List<Setting<String>> unknownSettings = new ArrayList<>(numberOfUnknownSettings);
        for (int i = 0; i < numberOfUnknownSettings; i++) {
            unknownSettings.add(Setting.simpleString("unknown.setting" + i, Property.NodeScope, Property.ProjectScope));
        }
        return unknownSettings;
    }

    private static List<Setting<String>> invalidSettings(int numberOfInvalidSettings) {
        final List<Setting<String>> invalidSettings = new ArrayList<>(numberOfInvalidSettings);
        for (int i = 0; i < numberOfInvalidSettings; i++) {
            invalidSettings.add(randomBoolean() ? invalidInIsolationSetting(i) : invalidWithDependenciesSetting(i));
        }
        return invalidSettings;
    }

    private static Setting<String> invalidInIsolationSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<>() {

            @Override
            public void validate(final String value) {
                throw new IllegalArgumentException("Invalid in isolation setting");
            }

        }, Property.NodeScope, Property.ProjectScope);
    }

    private static Setting<String> invalidWithDependenciesSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<>() {

            @Override
            public void validate(final String value) {}

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {
                throw new IllegalArgumentException("Invalid with dependencies setting");
            }

        }, Property.NodeScope, Property.ProjectScope);
    }

    private static class FooLowSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {}

        @Override
        public void validate(final Integer low, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_HIGH) && low > (int) settings.get(SETTING_FOO_HIGH)) {
                throw new IllegalArgumentException("[low]=" + low + " is higher than [high]=" + settings.get(SETTING_FOO_HIGH));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(SETTING_FOO_HIGH);
            return settings.iterator();
        }

    }

    private static class FooHighSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {

        }

        @Override
        public void validate(final Integer high, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_LOW) && high < (int) settings.get(SETTING_FOO_LOW)) {
                throw new IllegalArgumentException("[high]=" + high + " is lower than [low]=" + settings.get(SETTING_FOO_LOW));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(SETTING_FOO_LOW);
            return settings.iterator();
        }

    }

    private static final Setting<Integer> SETTING_FOO_LOW = new Setting<>(
        "foo.low",
        "10",
        Integer::valueOf,
        new FooLowSettingValidator(),
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );
    private static final Setting<Integer> SETTING_FOO_HIGH = new Setting<>(
        "foo.high",
        "100",
        Integer::valueOf,
        new FooHighSettingValidator(),
        Property.Dynamic,
        Property.NodeScope,
        Property.ProjectScope
    );

    public void testUpdateOfValidationDependentSettings() {
        final ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(
            Settings.EMPTY,
            new HashSet<>(asList(SETTING_FOO_LOW, SETTING_FOO_HIGH))
        );
        final ProjectSettingsUpdater updater = new ProjectSettingsUpdater(projectScopedSettings);
        ProjectState projectState = projectWithSettings(Settings.EMPTY);

        ProjectId projectId = projectState.projectId();
        Settings newSettings = updater.updateProjectSettings(
            projectId,
            projectState.settings(),
            Settings.builder().put(SETTING_FOO_LOW.getKey(), 20).build(),
            logger
        );
        assertThat(newSettings.get(SETTING_FOO_LOW.getKey()), equalTo("20"));

        newSettings = updater.updateProjectSettings(
            projectId,
            newSettings,
            Settings.builder().put(SETTING_FOO_HIGH.getKey(), 40).build(),
            logger
        );
        assertThat(newSettings.get(SETTING_FOO_LOW.getKey()), equalTo("20"));
        assertThat(newSettings.get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        newSettings = updater.updateProjectSettings(
            projectId,
            newSettings,
            Settings.builder().put(SETTING_FOO_LOW.getKey(), 5).build(),
            logger
        );
        assertThat(newSettings.get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(newSettings.get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        newSettings = updater.updateProjectSettings(
            projectId,
            newSettings,
            Settings.builder().put(SETTING_FOO_HIGH.getKey(), 8).build(),
            logger
        );
        assertThat(newSettings.get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(newSettings.get(SETTING_FOO_HIGH.getKey()), equalTo("8"));

        final Settings finalSettings = newSettings;
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateProjectSettings(
                projectId,
                finalSettings,
                Settings.builder().put(SETTING_FOO_HIGH.getKey(), 2).build(),
                logger
            )
        );

        assertThat(
            exception.getMessage(),
            either(equalTo("[high]=2 is lower than [low]=5")).or(equalTo("[low]=5 is higher than [high]=2"))
        );
    }

    public void testNotExistingSetting() {
        ProjectScopedSettings projectScopedSettings = new ProjectScopedSettings(Settings.EMPTY, Set.of(SETTING_A));
        ProjectSettingsUpdater updater = new ProjectSettingsUpdater(projectScopedSettings);
        ProjectState projectState = projectWithSettings(Settings.EMPTY);
        Setting<Float> notExistingSetting = Setting.floatSetting("not.existing.setting", 0.1f, 0.0f, Property.Dynamic, Property.NodeScope);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateProjectSettings(
                projectState.projectId(),
                projectState.settings(),
                Settings.builder().put(notExistingSetting.getKey(), "value").build(),
                logger
            )
        );
        assertThat(
            "project[" + projectState.projectId() + "] setting [" + notExistingSetting.getKey() + "], not recognized",
            equalTo(ex.getMessage())
        );
    }
}
