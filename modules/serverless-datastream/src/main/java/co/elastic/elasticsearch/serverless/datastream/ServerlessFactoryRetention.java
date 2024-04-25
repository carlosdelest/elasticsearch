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

package co.elastic.elasticsearch.serverless.datastream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.DataStreamFactoryRetention;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Provides the factory retention configuration for the serverless distribution. It reads the
 * values from the file-based settings and monitors them for changes. Finally, it validates the
 * setting values to ensure that they will be converted to valid global retention settings.
 */
public class ServerlessFactoryRetention implements DataStreamFactoryRetention {

    private static final Logger logger = LogManager.getLogger(ServerlessFactoryRetention.class);

    public static final Setting<TimeValue> DATA_STREAMS_DEFAULT_RETENTION_SETTING = Setting.timeSetting(
        "data_streams.lifecycle.retention.factory_default",
        TimeValue.MINUS_ONE,
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {}

            @Override
            public void validate(final TimeValue settingValue, final Map<Setting<?>, Object> settings) {
                TimeValue defaultRetention = getSettingValueOrNull(settingValue);
                TimeValue maxRetention = getSettingValueOrNull((TimeValue) settings.get(DATA_STREAMS_MAX_RETENTION_SETTING));
                // We want to ensure that the configuration will yield valid global retention
                new DataStreamGlobalRetention(defaultRetention, maxRetention);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(DATA_STREAMS_MAX_RETENTION_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> DATA_STREAMS_MAX_RETENTION_SETTING = Setting.timeSetting(
        "data_streams.lifecycle.retention.factory_max",
        TimeValue.MINUS_ONE,
        new Setting.Validator<>() {
            @Override
            public void validate(TimeValue value) {}

            @Override
            public void validate(final TimeValue settingValue, final Map<Setting<?>, Object> settings) {
                TimeValue defaultRetention = getSettingValueOrNull((TimeValue) settings.get(DATA_STREAMS_DEFAULT_RETENTION_SETTING));
                TimeValue maxRetention = getSettingValueOrNull(settingValue);
                // We want to ensure that the configuration will yield valid global retention
                new DataStreamGlobalRetention(defaultRetention, maxRetention);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(DATA_STREAMS_DEFAULT_RETENTION_SETTING);
                return settings.iterator();
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Nullable
    private volatile TimeValue defaultRetention;
    @Nullable
    private volatile TimeValue maxRetention;

    @Nullable
    @Override
    public TimeValue getMaxRetention() {
        return maxRetention;
    }

    @Nullable
    @Override
    public TimeValue getDefaultRetention() {
        return defaultRetention;
    }

    @Override
    public void init(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(DATA_STREAMS_DEFAULT_RETENTION_SETTING, this::setDefaultRetention);
        clusterSettings.initializeAndWatch(DATA_STREAMS_MAX_RETENTION_SETTING, this::setMaxRetention);
    }

    private void setMaxRetention(TimeValue maxRetention) {
        this.maxRetention = getSettingValueOrNull(maxRetention);
        logger.info("Updated max factory retention to [{}]", this.maxRetention == null ? null : maxRetention.getStringRep());
    }

    private void setDefaultRetention(TimeValue defaultRetention) {
        this.defaultRetention = getSettingValueOrNull(defaultRetention);
        logger.info("Updated default factory retention to [{}]", this.defaultRetention == null ? null : defaultRetention.getStringRep());
    }

    /**
     * Time value settings do not accept null as a value. To represent an undefined retention as a setting we use the value
     * of <code>-1</code> and this method converts this to null.
     *
     * @param value the retention as parsed from the setting
     * @return the value when it is not -1 and null otherwise
     */
    @Nullable
    private static TimeValue getSettingValueOrNull(TimeValue value) {
        return value.equals(TimeValue.MINUS_ONE) ? null : value;
    }
}
