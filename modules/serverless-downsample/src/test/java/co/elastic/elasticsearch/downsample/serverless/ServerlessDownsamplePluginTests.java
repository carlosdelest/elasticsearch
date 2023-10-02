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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class ServerlessDownsamplePluginTests extends ESTestCase {

    public void testSettings() {
        final Plugin plugin = new ServerlessDownsamplePlugin();
        assertThat(plugin.getSettings(), Matchers.hasSize(1));
        final Setting<?> setting = plugin.getSettings().get(0);
        assertThat(setting.getKey(), Matchers.equalTo("downsample.min_number_of_replicas"));
        assertThat(setting.getDefault(Settings.EMPTY), Matchers.equalTo(1));
    }
}
