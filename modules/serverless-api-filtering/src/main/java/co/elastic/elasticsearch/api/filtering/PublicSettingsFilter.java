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

package co.elastic.elasticsearch.api.filtering;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;

/**
 * A class that will perform filtering of non Setting.Property.ServerlessPublic
 */
public class PublicSettingsFilter {

    private IndexScopedSettings indexScopedSettings;

    public PublicSettingsFilter(IndexScopedSettings indexScopedSettings) {
        this.indexScopedSettings = indexScopedSettings;

    }

    /**
     * For a public user (no operator privileges) if a setting does not a ServerlessPublic property
     * it should be filtered out from the Response's Setting
     *
     * @param settings - settings from the request
     * @return settings after filtering
     */
    public Settings filter(Settings settings) {
        var normalized = Settings.builder().put(settings).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        return normalized.filter(key -> {
            var setting = indexScopedSettings.get(key);
            return setting != null && setting.isServerlessPublic();
        });
    }
}
