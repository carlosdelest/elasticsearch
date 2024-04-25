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

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

import static co.elastic.elasticsearch.serverless.datastream.ServerlessFactoryRetention.DATA_STREAMS_DEFAULT_RETENTION_SETTING;
import static co.elastic.elasticsearch.serverless.datastream.ServerlessFactoryRetention.DATA_STREAMS_MAX_RETENTION_SETTING;
import static org.elasticsearch.action.datastreams.autosharding.DataStreamAutoShardingService.DATA_STREAMS_AUTO_SHARDING_ENABLED;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.index.mapper.SourceFieldMapper.LOSSY_PARAMETERS_ALLOWED_SETTING_NAME;

/**
 * Serverless plugin that registers data stream settings.
 */
public class ServerlessDataStreamPlugin extends Plugin {

    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = boolSetting(
        DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME,
        true,
        Property.NodeScope
    );
    public static final Setting<TimeValue> FAILURE_STORE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        MetadataCreateDataStreamService.FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME,
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    public static final Setting<Boolean> DATA_STREAM_AUTO_SHARDING_ENABLED_SETTING = boolSetting(
        DATA_STREAMS_AUTO_SHARDING_ENABLED,
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SOURCE_MAPPER_LOSSY_PARAMETERS_ALLOWED_SETTING = boolSetting(
        LOSSY_PARAMETERS_ALLOWED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    @Override
    public Settings additionalSettings() {
        // We explicitly configure these settings here, as they're read in the open source distribution where the
        // settings are not registered. We fall back to their default values in the open source distribution
        // and here we set the values for serverless.
        return Settings.builder()
            .put(DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey(), true)
            .put(FAILURE_STORE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30))
            .put(DATA_STREAMS_AUTO_SHARDING_ENABLED, true)
            .put(LOSSY_PARAMETERS_ALLOWED_SETTING_NAME, false)
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            DATA_STREAMS_LIFECYCLE_ONLY_MODE,
            FAILURE_STORE_REFRESH_INTERVAL_SETTING,
            DATA_STREAM_AUTO_SHARDING_ENABLED_SETTING,
            SOURCE_MAPPER_LOSSY_PARAMETERS_ALLOWED_SETTING,
            DATA_STREAMS_DEFAULT_RETENTION_SETTING,
            DATA_STREAMS_MAX_RETENTION_SETTING
        );
    }
}
