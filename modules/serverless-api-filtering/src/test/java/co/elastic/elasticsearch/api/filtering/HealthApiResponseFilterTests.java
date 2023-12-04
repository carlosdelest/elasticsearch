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

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.DiskHealthIndicatorService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class HealthApiResponseFilterTests extends ESTestCase {

    public void testFilterHealthApiAction() {
        int numberOfIndicators = randomIntBetween(1, 5);
        int position = randomIntBetween(0, numberOfIndicators - 1);
        List<HealthIndicatorResult> indicatorResults = new ArrayList<>(numberOfIndicators);
        for (int i = 0; i < numberOfIndicators; i++) {
            if (i == position) {
                String name = ShardsAvailabilityHealthIndicatorService.NAME;
                List<Diagnosis> diagnoses = randomDiagnoses(name);
                diagnoses.add(
                    new Diagnosis(
                        new Diagnosis.Definition(
                            name,
                            randomAlphaOfLength(30),
                            randomAlphaOfLength(50),
                            randomAlphaOfLength(50),
                            randomFrom(HealthApiResponseFilter.ShardsAvailabilityHealthIndicatorFilter.OVERRIDE_HELP_URL.keySet())
                        ),
                        List.of()
                    )
                );
                indicatorResults.add(
                    new HealthIndicatorResult(
                        name,
                        randomHealthStatus(),
                        randomAlphaOfLength(20),
                        randomDetails(),
                        randomImpacts(name),
                        diagnoses
                    )
                );
            } else {
                String name = randomAlphaOfLength(10);
                indicatorResults.add(
                    new HealthIndicatorResult(
                        name,
                        randomHealthStatus(),
                        randomAlphaOfLength(20),
                        randomDetails(),
                        randomImpacts(name),
                        randomDiagnoses(name)
                    )
                );
            }
        }
        GetHealthAction.Response response = new GetHealthAction.Response(ClusterName.DEFAULT, indicatorResults, randomHealthStatus());
        HealthApiResponseFilter filter = new HealthApiResponseFilter(new ThreadContext(Settings.EMPTY));
        GetHealthAction.Response filteredResponse = filter.filterResponse(response);
        assertThat(filteredResponse.getClusterName(), equalTo(response.getClusterName()));
        assertThat(filteredResponse.getStatus(), equalTo(response.getStatus()));
        assertThat(filteredResponse.getIndicatorResults().size(), equalTo(response.getIndicatorResults().size()));
        for (int i = 0; i < numberOfIndicators; i++) {
            HealthIndicatorResult original = response.getIndicatorResults().get(i);
            HealthIndicatorResult filtered = filteredResponse.getIndicatorResults().get(i);
            if (original.name().equals(ShardsAvailabilityHealthIndicatorService.NAME)) {
                assertThat(filtered.name(), equalTo(original.name()));
                assertThat(filtered.symptom(), equalTo(original.symptom()));
                assertThat(filtered.status(), equalTo(original.status()));
                assertThat(filtered.details(), equalTo(original.details()));
                assertThat(filtered.impacts(), equalTo(original.impacts()));
                for (int j = 0; j < original.diagnosisList().size(); j++) {
                    Diagnosis originalDiagnosis = original.diagnosisList().get(j);
                    Diagnosis filteredDiagnosis = filtered.diagnosisList().get(j);
                    if (HealthApiResponseFilter.ShardsAvailabilityHealthIndicatorFilter.OVERRIDE_HELP_URL.containsKey(
                        originalDiagnosis.definition().helpURL()
                    )) {
                        assertThat(filteredDiagnosis.affectedResources(), equalTo(originalDiagnosis.affectedResources()));
                        assertThat(filteredDiagnosis.definition().id(), equalTo(originalDiagnosis.definition().id()));
                        assertThat(filteredDiagnosis.definition().action(), equalTo(originalDiagnosis.definition().action()));
                        assertThat(filteredDiagnosis.definition().cause(), equalTo(originalDiagnosis.definition().cause()));
                        assertThat(filteredDiagnosis.definition().indicatorName(), equalTo(originalDiagnosis.definition().indicatorName()));
                        assertThat(
                            filteredDiagnosis.definition().helpURL(),
                            equalTo(
                                HealthApiResponseFilter.ShardsAvailabilityHealthIndicatorFilter.OVERRIDE_HELP_URL.get(
                                    originalDiagnosis.definition().helpURL()
                                )
                            )
                        );
                    } else {
                        assertThat(filteredDiagnosis, equalTo(originalDiagnosis));
                    }
                }
            } else {
                assertThat(filtered, equalTo(original));
            }
        }
    }

    public void testDiskHealthIndicatorFilter() {
        String name = DiskHealthIndicatorService.NAME;
        Diagnosis originalDiagnosis = new Diagnosis(
            new Diagnosis.Definition(
                name,
                randomBoolean() ? "add_disk_capacity_data_nodes" : "add_disk_capacity",
                randomAlphaOfLength(50),
                randomAlphaOfLength(50),
                randomAlphaOfLength(50)
            ),
            List.of()
        );
        List<Diagnosis> diagnoses = List.of(originalDiagnosis);
        HealthIndicatorResult result = new HealthIndicatorResult(
            name,
            randomHealthStatus(),
            randomAlphaOfLength(20),
            randomDetails(),
            randomImpacts(name),
            diagnoses
        );
        HealthIndicatorResult filtered = HealthApiResponseFilter.DiskHealthIndicatorFilter.filter(result);
        Diagnosis filteredDiagnosis = filtered.diagnosisList().get(0);
        assertThat(filteredDiagnosis.affectedResources(), equalTo(originalDiagnosis.affectedResources()));
        assertThat(filteredDiagnosis.definition().id(), equalTo(originalDiagnosis.definition().id()));
        assertThat(filteredDiagnosis.definition().action(), containsString("Please, look into the logs to figure out what went wrong."));
        assertThat(filteredDiagnosis.definition().cause(), equalTo(originalDiagnosis.definition().cause()));
        assertThat(filteredDiagnosis.definition().indicatorName(), equalTo(originalDiagnosis.definition().indicatorName()));
        assertThat(filteredDiagnosis.definition().helpURL(), equalTo(HealthApiResponseFilter.DiskHealthIndicatorFilter.UPDATED_HELP_URL));
    }

    private static HealthStatus randomHealthStatus() {
        return randomFrom(HealthStatus.RED, HealthStatus.YELLOW, HealthStatus.GREEN);
    }

    private static SimpleHealthIndicatorDetails randomDetails() {
        Map<String, Object> detailsMap = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 10); i++) {
            detailsMap.put(randomAlphaOfLengthBetween(5, 50), randomAlphaOfLengthBetween(5, 50));
        }
        return new SimpleHealthIndicatorDetails(detailsMap);
    }

    private static List<HealthIndicatorImpact> randomImpacts(String name) {
        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String impactId = randomAlphaOfLength(30);
            int impactSeverity = randomIntBetween(1, 5);
            String impactDescription = randomAlphaOfLength(30);
            List<ImpactArea> impactAreas = randomNonEmptySubsetOf(Arrays.stream(ImpactArea.values()).toList());
            impacts.add(new HealthIndicatorImpact(name, impactId, impactSeverity, impactDescription, impactAreas));
        }
        return impacts;
    }

    private static List<Diagnosis> randomDiagnoses(String name) {
        List<Diagnosis> diagnoses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            Diagnosis.Resource resource = new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of(randomAlphaOfLength(10)));
            Diagnosis diagnosis = new Diagnosis(
                new Diagnosis.Definition(
                    name,
                    randomAlphaOfLength(30),
                    randomAlphaOfLength(50),
                    randomAlphaOfLength(50),
                    randomAlphaOfLength(30)
                ),
                List.of(resource)
            );
            diagnoses.add(diagnosis);
        }
        return diagnoses;
    }
}
