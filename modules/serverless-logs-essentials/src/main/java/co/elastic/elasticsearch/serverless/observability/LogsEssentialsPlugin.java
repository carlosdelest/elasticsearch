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

package co.elastic.elasticsearch.serverless.observability;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.serverless.observability.api.LogsEssentialsAsyncSearchRequestValidator;
import co.elastic.elasticsearch.serverless.observability.api.LogsEssentialsSearchRequestValidator;
import co.elastic.elasticsearch.serverless.observability.api.MlXpackInfoApiFilter;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LogsEssentialsPlugin extends Plugin implements ActionPlugin {
    private final SetOnce<Settings> settings = new SetOnce<>();
    private final SetOnce<ThreadContext> threadContext = new SetOnce<>();

    @Override
    public Collection<?> createComponents(PluginServices services) {
        settings.set(services.clusterService().getSettings());
        threadContext.set(services.threadPool().getThreadContext());
        return Collections.emptyList();
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        boolean logsEssentials = ServerlessSharedSettings.OBSERVABILITY_TIER.get(settings.get()) == ObservabilityTier.LOGS_ESSENTIALS;
        return (logsEssentials)
            ? List.of(
                new LogsEssentialsAsyncSearchRequestValidator(settings.get()),
                new LogsEssentialsSearchRequestValidator(settings.get()),
                new MlXpackInfoApiFilter(threadContext.get(), settings.get())
            )
            : Collections.emptyList();
    }
}
