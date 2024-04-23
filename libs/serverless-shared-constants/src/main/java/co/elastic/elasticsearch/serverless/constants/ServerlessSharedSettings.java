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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Settings that may be read across multiple serverless modules.
 */
public class ServerlessSharedSettings {

    public static final Setting<TimeValue> BOOST_WINDOW_SETTING = Setting.timeSetting(
        "serverless.search.boost_window",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueDays(1),
        TimeValue.timeValueDays(365),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> SEARCH_POWER_SETTING = Setting.intSetting(
        // to be removed in future PR `serverless.search.search_power`, we keep it for now for BWC
        // is it currently only used as a fallback settings in case min-max are not defined.
        "serverless.search.search_power",
        100,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> SEARCH_POWER_MIN_SETTING = Setting.intSetting(
        "serverless.search.search_power_min",
        SEARCH_POWER_SETTING,
        0,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(final Integer searchPowerMin, final Map<Setting<?>, Object> settings) {
                int searchPowerMax = (int) settings.get(SEARCH_POWER_MAX_SETTING);
                if (searchPowerMin > searchPowerMax) {
                    throw new IllegalArgumentException(
                        ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                            + " ["
                            + searchPowerMin
                            + "] must be <= "
                            + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                            + " ["
                            + searchPowerMax
                            + "]"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(SEARCH_POWER_MAX_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> SEARCH_POWER_MAX_SETTING = Setting.intSetting(
        "serverless.search.search_power_max",
        SEARCH_POWER_MIN_SETTING,
        0,
        new Setting.Validator<>() {
            @Override
            public void validate(Integer value) {}

            @Override
            public void validate(final Integer searchPowerMax, final Map<Setting<?>, Object> settings) {
                int searchPowerMin = (int) settings.get(SEARCH_POWER_MIN_SETTING);
                if (searchPowerMax < searchPowerMin) {
                    throw new IllegalArgumentException(
                        ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING.getKey()
                            + " ["
                            + searchPowerMin
                            + "] must be <= "
                            + ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING.getKey()
                            + " ["
                            + searchPowerMax
                            + "]"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(SEARCH_POWER_MIN_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // TODO: This setting name is what the ES controller passes currently.
    // Remove once the controller is changed to pass with the serverless prefix.
    static final Setting<String> BWC_PROJECT_ID = Setting.simpleString("metering.project_id", Setting.Property.NodeScope);

    public static final Setting<String> PROJECT_ID = Setting.simpleString(
        "serverless.project_id",
        BWC_PROJECT_ID,
        Setting.Property.NodeScope
    );

    public static final Setting<ProjectType> PROJECT_TYPE = Setting.enumSetting(
        ProjectType.class,
        "serverless.project_type",
        ProjectType.ELASTICSEARCH_SEARCH,
        Setting.Property.NodeScope
    );

    private ServerlessSharedSettings() {}
}
