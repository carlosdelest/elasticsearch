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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.serverless.constants.ObservabilityTier.ESSENTIALS;

/**
 * Settings that may be read across multiple serverless modules.
 */
public class ServerlessSharedSettings {
    private static final int SEARCH_POWER_MIN_DEFAULT_VALUE = 100;
    private static final int SEARCH_POWER_MINIMUM_VALUE = 0;

    public static final Setting<TimeValue> BOOST_WINDOW_SETTING = Setting.timeSetting(
        "serverless.search.boost_window",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueDays(1),
        TimeValue.timeValueDays(365),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> SEARCH_POWER_MIN_SETTING = Setting.intSetting(
        "serverless.search.search_power_min",
        SEARCH_POWER_MIN_DEFAULT_VALUE,
        SEARCH_POWER_MINIMUM_VALUE,
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
        SEARCH_POWER_MINIMUM_VALUE,
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
    public static final Setting<Boolean> ENABLE_REPLICAS_FOR_INSTANT_FAILOVER = Setting.boolSetting(
        "serverless.search.enable_replicas_for_instant_failover",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> PROJECT_ID = Setting.simpleString("serverless.project_id", Setting.Property.NodeScope);

    public static final Setting<ProjectType> PROJECT_TYPE = Setting.enumSetting(
        ProjectType.class,
        "serverless.project_type",
        ProjectType.ELASTICSEARCH_GENERAL_PURPOSE,
        Setting.Property.NodeScope
    );

    /**
     * Stores the number of vCPUs configured by the current step of the Autoscaling Step Function (set as the k8s cpu "request"). This is
     * set automatically via the cgroup cpu.weight/shares value.
     */
    public static final Setting<Double> VCPU_REQUEST = Setting.doubleSetting(
        "node.vcpu_request",
        Runtime.getRuntime().availableProcessors(),
        // smallest *positive* double
        Double.MIN_VALUE,
        Double.POSITIVE_INFINITY,
        Setting.Property.NodeScope
    );

    public static final Setting<ObservabilityTier> OBSERVABILITY_TIER = Setting.enumSetting(
        ObservabilityTier.class,
        "serverless.observability.tier",
        ObservabilityTier.COMPLETE,
        new Setting.Validator<>() {
            @Override
            public void validate(ObservabilityTier value) {}

            @Override
            public void validate(ObservabilityTier value, Map<Setting<?>, Object> settings) {
                ProjectType projectType = (ProjectType) settings.get(PROJECT_TYPE);

                // Observability tier can only be set to essentials for observability projects
                if (value == ESSENTIALS && projectType != ProjectType.OBSERVABILITY) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Setting [%s] may only be set to [%s] when project type is [%s].",
                            OBSERVABILITY_TIER.getKey(),
                            value,
                            ProjectType.OBSERVABILITY
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> settings = List.of(PROJECT_TYPE);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope
    );

    private ServerlessSharedSettings() {}
}
