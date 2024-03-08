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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * Serverless plugin that registers the data stream settings.
 */
public class ServerlessDataStreamPlugin extends Plugin {

    private static final String DATA_STREAMS_LIFECYCLE_ONLY_MODE_NAME = "data_streams.lifecycle_only.mode";
    public static final Setting<Boolean> DATA_STREAMS_LIFECYCLE_ONLY_MODE = Setting.boolSetting(
        "data_streams.lifecycle_only.mode",
        true,
        Property.NodeScope
    );

    @Override
    public Settings additionalSettings() {
        // we explicitly configure the setting here as it's read in the open source distribution where the
        // setting is not registered (and it's read with a default of `false`)
        // this way we'll be able to make sure the in serverless the setting will always have the value `true`
        return Settings.builder().put(DATA_STREAMS_LIFECYCLE_ONLY_MODE_NAME, true).build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DATA_STREAMS_LIFECYCLE_ONLY_MODE);
    }
}
