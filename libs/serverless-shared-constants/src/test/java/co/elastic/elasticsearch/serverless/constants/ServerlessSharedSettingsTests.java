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

package co.elastic.elasticsearch.serverless.constants;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class ServerlessSharedSettingsTests extends ESTestCase {
    protected static final TimeValue DEFAULT_BOOST_WINDOW = TimeValue.timeValueDays(2);

    public void testDefaultSettings() {
        int defaultSP = 100;
        assertEquals(TimeValue.timeValueDays(7), ServerlessSharedSettings.BOOST_WINDOW_SETTING.get(Settings.EMPTY));
        assertEquals(defaultSP, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(Settings.EMPTY).intValue());
        assertEquals(defaultSP, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(Settings.EMPTY).intValue());
    }

    public void testSettingsOnlySPMinProvided() {
        int spMin = 150;
        Settings s = Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin).build();
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
    }

    public void testSettingsOnlySPMaxProvided() {
        int spMax = 150;
        int defaultSP = 100;
        Settings s = Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax).build();
        assertEquals(defaultSP, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMax, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
    }

    public void testSettingsWithSPMaxAndSPMinProvided() {
        int spMin = 50;
        int spMax = 150;
        Settings s = Settings.builder()
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax)
            .build();
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMax, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
    }

    public void testSettingsSPMinGreaterThanSPMax() {
        int spMin = 50;
        int spMax = 42;
        Settings s = Settings.builder()
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue()
        );
        assertEquals(
            e.getMessage(),
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                + " ["
                + spMin
                + "] must be <= "
                + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                + " ["
                + spMax
                + "]"
        );
    }

    public void testUpdateSPMin() {
        int spMin = 50;
        int spMax = 150;
        int spSelected = randomIntBetween(spMin, spMax);
        Settings s = Settings.builder()
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax)
            .build();
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMax, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
        int newSPMin = 20;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING
            )
        );
        Settings update = Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), newSPMin).build();
        assertTrue(clusterSettings.updateSettings(update, Settings.builder().put(s), Settings.builder(), ""));
    }

    public void testUpdateSPMax() {
        int spMin = 50;
        int spMax = 150;
        int spSelected = randomIntBetween(spMin, spMax);
        Settings s = Settings.builder()
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax)
            .build();
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMax, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
        int newSPMax = 200;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING
            )
        );
        Settings update = Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), newSPMax).build();
        assertTrue(clusterSettings.updateSettings(update, Settings.builder().put(s), Settings.builder(), ""));
    }

    public void testFailingUpdateSPMin() {
        int spMin = 50;
        int spMax = 150;
        int spSelected = randomIntBetween(spMin, spMax);
        Settings s = Settings.builder()
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax)
            .build();
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMax, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
        int newSPMin = 200;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING
            )
        );
        Settings.Builder sb = Settings.builder().put(s);
        Settings update = Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), newSPMin).build();
        clusterSettings.updateSettings(update, sb, Settings.builder(), "");
        Settings updatedSettings = sb.build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(updatedSettings).intValue()
        );
        assertEquals(
            e.getMessage(),
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                + " ["
                + newSPMin
                + "] must be <= "
                + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                + " ["
                + spMax
                + "]"
        );
    }

    public void testFailingUpdateSPMax() {
        int spMin = 50;
        int spMax = 150;
        int spSelected = randomIntBetween(spMin, spMax);
        Settings s = Settings.builder()
            .put(ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey(), spMin)
            .put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), spMax)
            .build();
        assertEquals(spMin, ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.get(s).intValue());
        assertEquals(spMax, ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(s).intValue());
        int newSPMax = 20;
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
                ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING
            )
        );
        Settings.Builder sb = Settings.builder().put(s);
        Settings update = Settings.builder().put(ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey(), newSPMax).build();
        clusterSettings.updateSettings(update, sb, Settings.builder(), "");
        Settings updatedSettings = sb.build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.get(updatedSettings).intValue()
        );
        assertEquals(
            e.getMessage(),
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                + " ["
                + spMin
                + "] must be <= "
                + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                + " ["
                + newSPMax
                + "]"
        );
    }

    public void testFailingObservabilityTierOnNonObservabilityProjects() {
        var unsupportedTypes = Arrays.stream(ProjectType.values()).filter(t -> t != ProjectType.OBSERVABILITY).toList();
        var settings = Settings.builder()
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), randomFrom(unsupportedTypes))
            .put(ServerlessSharedSettings.OBSERVABILITY_TIER.getKey(), ObservabilityTier.ESSENTIALS.name())
            .build();

        var exception = expectThrows(IllegalArgumentException.class, () -> ServerlessSharedSettings.OBSERVABILITY_TIER.get(settings));
        assertThat(
            exception.getMessage(),
            is("Setting [serverless.observability.tier] may only be set to [ESSENTIALS] when project type is [OBSERVABILITY].")
        );
    }
}
