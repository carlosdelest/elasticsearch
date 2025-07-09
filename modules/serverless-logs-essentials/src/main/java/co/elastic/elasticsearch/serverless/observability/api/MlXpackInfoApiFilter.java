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

package co.elastic.elasticsearch.serverless.observability.api;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureResponse;

public class MlXpackInfoApiFilter implements MappedActionFilter {
    private final boolean isLogsEssentialsProject;

    public MlXpackInfoApiFilter(Settings settings) {
        this.isLogsEssentialsProject = ServerlessSharedSettings.OBSERVABILITY_TIER.get(settings) == ObservabilityTier.LOGS_ESSENTIALS;
    }

    @Override
    public String actionName() {
        return XPackInfoFeatureAction.MACHINE_LEARNING.name();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        chain.proceed(task, action, request, listener.map(resp -> {
            if (resp instanceof XPackInfoFeatureResponse && isLogsEssentialsProject) {
                return (Response) new XPackInfoFeatureResponse(
                    new XPackInfoResponse.FeatureSetsInfo.FeatureSet(XPackField.MACHINE_LEARNING, false, false)
                );
            } else {
                return resp;
            }
        }));
    }
}
