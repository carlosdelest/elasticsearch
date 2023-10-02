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

package co.elastic.elasticsearch.downsample.serverless;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * Serverless plugin that registers the downsample setting.
 */
public class ServerlessDownsamplePlugin extends Plugin {

    private static final String DOWNSMPLE_MIN_NUMBER_OF_REPLICAS_NAME = "downsample.min_number_of_replicas";
    public static final Setting<Integer> DOWNSAMPLE_MIN_NUMBER_OF_REPLICAS = Setting.intSetting(
        "downsample.min_number_of_replicas",
        1,
        Property.NodeScope
    );

    @Override
    public Settings additionalSettings() {
        // we explicitly configure the setting here as it's read in Elasticsearch public distribution where the
        // setting is not registered (and it's read with a default of `0`).
        // This way we'll be able to make sure the in serverless the setting will always have the value `1`.
        // Having at least one replica in the search tier in the Serverless distribution is required since the
        // downsampling persistent task recovery mechanism tries to read the target index upon restarting a
        // failed downsampling task.
        return Settings.builder().put(DOWNSMPLE_MIN_NUMBER_OF_REPLICAS_NAME, 1).build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DOWNSAMPLE_MIN_NUMBER_OF_REPLICAS);
    }
}
