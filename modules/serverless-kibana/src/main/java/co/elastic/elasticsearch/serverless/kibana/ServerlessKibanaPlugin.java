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

package co.elastic.elasticsearch.serverless.kibana;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class ServerlessKibanaPlugin extends Plugin {

    private final Pattern pattern = Pattern.compile(
        "\\.kibana_\\d+|"
            + "\\.kibana_alerting_cases_\\d+|"
            + "\\.kibana_analytics_\\d+|"
            + "\\.kibana_ingest_\\d+|"
            + "\\.kibana_task_manager_\\d+|"
            + "\\.kibana_security_solution_\\d+|"
            + "\\.apm-custom-link"
    );
    private final Predicate<String> isKibanaFastRefreshIndex = pattern.asMatchPredicate();

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of((indexName, dataStreamName, timeSeries, metadata, resolvedAt, allSettings, combinedTemplateMappings) -> {
            if (isKibanaFastRefreshIndex.test(indexName)) {
                return Settings.builder().put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build();

            } else {
                return Settings.EMPTY;
            }
        });
    }
}
