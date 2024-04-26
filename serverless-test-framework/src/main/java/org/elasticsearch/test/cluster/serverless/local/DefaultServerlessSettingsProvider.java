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

package org.elasticsearch.test.cluster.serverless.local;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class DefaultServerlessSettingsProvider extends DefaultSettingsProvider {
    private static final List<String> EXCLUDED_SETTINGS = List.of("cluster.deprecation_indexing.enabled");

    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        Map<String, String> defaultSettings = super.get(nodeSpec);
        final Random random = RandomizedContext.current().getRandom();
        defaultSettings.put("stateless.upload.delayed", String.valueOf(random.nextBoolean()));
        EXCLUDED_SETTINGS.forEach(defaultSettings::remove);
        return defaultSettings;
    }
}
