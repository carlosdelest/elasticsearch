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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureResponse;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

public class MlXpackInfoApiFilter extends ApiFilteringActionFilter<XPackInfoFeatureResponse> {
    private final boolean isLogsEssentialsProject;

    public MlXpackInfoApiFilter(ThreadContext context, Settings settings) {
        super(context, XPackInfoFeatureAction.MACHINE_LEARNING.name(), XPackInfoFeatureResponse.class, true);
        this.isLogsEssentialsProject = ServerlessSharedSettings.OBSERVABILITY_TIER.get(settings) == ObservabilityTier.LOGS_ESSENTIALS;
    }

    @Override
    protected XPackInfoFeatureResponse filterResponse(XPackInfoFeatureResponse response) throws Exception {
        if (isLogsEssentialsProject && response.getInfo().name().equals(XPackField.MACHINE_LEARNING)) {
            return new XPackInfoFeatureResponse(
                new XPackInfoResponse.FeatureSetsInfo.FeatureSet(XPackField.MACHINE_LEARNING, false, false)
            );
        }

        return response;
    }
}
