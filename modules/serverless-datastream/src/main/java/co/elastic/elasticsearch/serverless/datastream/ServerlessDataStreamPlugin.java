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

/**
 * Serverless plugin that registers data stream settings.
 */
public class ServerlessDataStreamPlugin extends Plugin {

    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = Setting.boolSetting(
        DataStreamLifecycle.DATA_STREAMS_LIFECYCLE_ONLY_SETTING_NAME,
        true,
        Property.NodeScope
    );
    public static final Setting<TimeValue> FAILURE_STORE_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        MetadataCreateDataStreamService.FAILURE_STORE_REFRESH_INTERVAL_SETTING_NAME,
        TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    @Override
    public Settings additionalSettings() {
        // We explicitly configure these settings here, as they're read in the open source distribution where the
        // settings are not registered. We fall back to their default values in the open source distribution
        // and here we set the values for serverless.
        return Settings.builder()
            .put(DATA_STREAMS_LIFECYCLE_ONLY_MODE.getKey(), true)
            .put(FAILURE_STORE_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30))
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DATA_STREAMS_LIFECYCLE_ONLY_MODE, FAILURE_STORE_REFRESH_INTERVAL_SETTING);
    }
}
