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

import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.xpack.core.api.filtering.ApiFilteringActionFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This ApiFilteringActionFilter manipulates the response of the Health API to provide information curated for "serverless".
 * Currently, it overrides help urls.
 */
public class HealthApiResponseFilter extends ApiFilteringActionFilter<GetHealthAction.Response> {

    static final Map<String, String> OVERRIDE_HELP_URL = Map.of(
        ShardsAvailabilityHealthIndicatorService.ENABLE_INDEX_ALLOCATION_GUIDE,
        "https://ela.st/serverless-fix-index-allocation",
        ShardsAvailabilityHealthIndicatorService.ENABLE_CLUSTER_ALLOCATION_ACTION_GUIDE,
        "https://ela.st/serverless-fix-cluster-allocation",
        ShardsAvailabilityHealthIndicatorService.RESTORE_FROM_SNAPSHOT_ACTION_GUIDE,
        "https://ela.st/serverless-restore-snapshot"
    );

    private static final Set<String> ADJUSTED_INDICATORS = Set.of(ShardsAvailabilityHealthIndicatorService.NAME);

    public HealthApiResponseFilter(ThreadContext threadContext) {
        super(threadContext, GetHealthAction.NAME, GetHealthAction.Response.class);
    }

    /*
     * This method replaces the help urls, aka the troubleshooting guides with guides that are adjusted for the
     * "serverless" set-up.
     */
    @Override
    protected GetHealthAction.Response filterResponse(GetHealthAction.Response response) {
        return new GetHealthAction.Response(
            response.getClusterName(),
            updateIndicatorResults(response.getIndicatorResults()),
            response.getStatus()
        );
    }

    private static List<HealthIndicatorResult> updateIndicatorResults(List<HealthIndicatorResult> indicatorResults) {
        if (indicatorResults == null || indicatorResults.isEmpty()) {
            return indicatorResults;
        }
        List<HealthIndicatorResult> updatedResults = new ArrayList<>(indicatorResults.size());
        for (HealthIndicatorResult indicatorResult : indicatorResults) {
            if (ADJUSTED_INDICATORS.contains(indicatorResult.name())) {
                updatedResults.add(
                    new HealthIndicatorResult(
                        indicatorResult.name(),
                        indicatorResult.status(),
                        indicatorResult.symptom(),
                        indicatorResult.details(),
                        indicatorResult.impacts(),
                        updateDiagnoses(indicatorResult.diagnosisList())
                    )
                );
            } else {
                updatedResults.add(indicatorResult);
            }
        }
        return updatedResults;
    }

    private static List<Diagnosis> updateDiagnoses(List<Diagnosis> diagnoses) {
        if (diagnoses == null || diagnoses.isEmpty()) {
            return diagnoses;
        }
        return diagnoses.stream().map(HealthApiResponseFilter::updateDiagnosis).toList();
    }

    private static Diagnosis updateDiagnosis(Diagnosis diagnosis) {
        if (OVERRIDE_HELP_URL.containsKey(diagnosis.definition().helpURL())) {
            return new Diagnosis(
                new Diagnosis.Definition(
                    diagnosis.definition().indicatorName(),
                    diagnosis.definition().id(),
                    diagnosis.definition().cause(),
                    diagnosis.definition().action(),
                    OVERRIDE_HELP_URL.get(diagnosis.definition().helpURL())
                ),
                diagnosis.affectedResources()
            );
        }
        return diagnosis;
    }
}
