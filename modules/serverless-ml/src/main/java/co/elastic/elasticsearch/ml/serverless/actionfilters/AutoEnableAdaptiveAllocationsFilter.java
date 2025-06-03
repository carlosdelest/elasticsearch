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

package co.elastic.elasticsearch.ml.serverless.actionfilters;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.ADAPTIVE_ALLOCATIONS;

/**
 * For serverless, Adaptive Allocations will always be enabled for the Start Model call. This filter will enable it if it is disabled.
 * We want to prioritize Adaptive Allocations because it autoscales based on ML traffic and is directly tied to user cost. Serverless will
 * add or remove ML nodes based on the number of allocations, so if a user launches a model and then does not use it, their ML nodes will
 * automatically downscale and reduce their costs. Users who want to disable Adaptive Allocations will have to start the model and then
 * update to disable it, or they can call Start Model with the same number of min and max allocations.
 */
public class AutoEnableAdaptiveAllocationsFilter implements MappedActionFilter {

    private static final int DEFAULT_MIN_NUMBER_OF_ALLOCATIONS = 0;
    private static final int DEFAULT_MAX_NUMBER_OF_ALLOCATIONS = 32;
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(AutoEnableAdaptiveAllocationsFilter.class);
    private static final String AUTO_ENABLING_ADAPTIVE_ALLOCATIONS =
        "Automatically enabling adaptive allocations with min [{}] and max [{}].";
    private static final String PREVENT_DISABLING_ADAPTIVE_ALLOCATIONS =
        "Serverless does not support disabling adaptive allocations when starting the model. "
            + "Enabling adaptive allocations with min [{}] and max [{}].";
    private static final String PREVENT_NONZERO_MIN_ALLOCATIONS =
        "Serverless does not support adaptive allocations with min allocations above 0. Setting min allocations to 0.";

    @Override
    public String actionName() {
        return StartTrainedModelDeploymentAction.NAME;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        assert request instanceof StartTrainedModelDeploymentAction.Request;
        var startTrainedModelRequest = (StartTrainedModelDeploymentAction.Request) request;

        var adaptiveAllocationsSettings = startTrainedModelRequest.getAdaptiveAllocationsSettings();
        var min = minNumberOfAllocations(adaptiveAllocationsSettings);
        var max = maxNumberOfAllocations(startTrainedModelRequest, adaptiveAllocationsSettings);
        startTrainedModelRequest.setAdaptiveAllocationsSettings(new AdaptiveAllocationsSettings(true, min, max));
        startTrainedModelRequest.setNumberOfAllocations(null); // must be null if adaptive allocations is enabled

        if (adaptiveAllocationsSettings == null || adaptiveAllocationsSettings.getEnabled() == false) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                ADAPTIVE_ALLOCATIONS.getPreferredName(),
                adaptiveAllocationsSettings == null ? AUTO_ENABLING_ADAPTIVE_ALLOCATIONS : PREVENT_DISABLING_ADAPTIVE_ALLOCATIONS,
                min,
                max
            );
        }

        chain.proceed(task, action, request, listener);
    }

    private int minNumberOfAllocations(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
        if (adaptiveAllocationsSettings != null
            && adaptiveAllocationsSettings.getMinNumberOfAllocations() != null
            && adaptiveAllocationsSettings.getMinNumberOfAllocations() != DEFAULT_MIN_NUMBER_OF_ALLOCATIONS) {
            deprecationLogger.warn(DeprecationCategory.API, ADAPTIVE_ALLOCATIONS.getPreferredName(), PREVENT_NONZERO_MIN_ALLOCATIONS);
        }
        // min allocations are always set to 0 in serverless
        // this is to minimize costs for users, at the risk of unavailability due to cold start
        return DEFAULT_MIN_NUMBER_OF_ALLOCATIONS;
    }

    private int maxNumberOfAllocations(
        StartTrainedModelDeploymentAction.Request request,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        if (adaptiveAllocationsSettings != null && adaptiveAllocationsSettings.getMaxNumberOfAllocations() != null) {
            return adaptiveAllocationsSettings.getMaxNumberOfAllocations();
        } else if (request.getNumberOfAllocations() != null) {
            return request.getNumberOfAllocations();
        } else {
            return DEFAULT_MAX_NUMBER_OF_ALLOCATIONS;
        }
    }
}
