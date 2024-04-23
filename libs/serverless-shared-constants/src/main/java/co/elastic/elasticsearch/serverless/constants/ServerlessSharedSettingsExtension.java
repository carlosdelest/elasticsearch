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
import org.elasticsearch.plugins.internal.SettingsExtension;

import java.util.List;

public class ServerlessSharedSettingsExtension implements SettingsExtension {
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ServerlessSharedSettings.BOOST_WINDOW_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MIN_SETTING,
            ServerlessSharedSettings.SEARCH_POWER_MAX_SETTING,
            ServerlessSharedSettings.BWC_PROJECT_ID,
            ServerlessSharedSettings.PROJECT_ID,
            ServerlessSharedSettings.PROJECT_TYPE
        );
    }
}
