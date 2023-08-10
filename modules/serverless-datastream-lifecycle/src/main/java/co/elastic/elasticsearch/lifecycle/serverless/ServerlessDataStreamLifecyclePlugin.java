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

package co.elastic.elasticsearch.lifecycle.serverless;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * Serverless plugin that registers the data stream lifecycle only setting.
 */
public class ServerlessDataStreamLifecyclePlugin extends Plugin {

    private static final String DATA_STREAMS_LIFECYCLE_ONLY_MODE_NAME = "data_streams.lifecycle_only.mode";
    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = Setting.boolSetting(
        "data_streams.lifecycle_only.mode",
        false,
        Property.NodeScope
    );

    public ServerlessDataStreamLifecyclePlugin(Settings settings) {
        if (DATA_STREAMS_LIFECYCLE_ONLY_MODE.get(settings) == false) {
            throw new IllegalArgumentException(
                DATA_STREAMS_LIFECYCLE_ONLY_MODE_NAME + " is not enabled and must be enable on serverless Elasticsearch"
            );
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DATA_STREAMS_LIFECYCLE_ONLY_MODE);
    }
}
