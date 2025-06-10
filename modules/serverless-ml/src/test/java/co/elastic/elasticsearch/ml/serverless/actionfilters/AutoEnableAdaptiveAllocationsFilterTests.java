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

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.mockito.Mockito;

import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

public class AutoEnableAdaptiveAllocationsFilterTests extends ESTestCase {
    private AutoEnableAdaptiveAllocationsFilter autoEnableFilter;
    private ActionFilterChain<StartTrainedModelDeploymentAction.Request, CreateTrainedModelAssignmentAction.Response> chain;
    private final TestCase testCase;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        autoEnableFilter = new AutoEnableAdaptiveAllocationsFilter(Settings.EMPTY);
        chain = mock();
    }

    public AutoEnableAdaptiveAllocationsFilterTests(TestCase testCase) {
        this.testCase = testCase;
    }

    public void testName() {
        assertThat(autoEnableFilter.actionName(), equalTo(StartTrainedModelDeploymentAction.NAME));
    }

    public void testWithAdaptiveAllocationsEnabled() {
        var request = createRandom(null, new AdaptiveAllocationsSettings(true, 9, 9));
        callFilter(request);
        verify(chain, only()).proceed(any(), eq(StartTrainedModelDeploymentAction.NAME), assertArg(actualRequest -> {
            assertTrue(actualRequest.getAdaptiveAllocationsSettings().getEnabled());
            assertThat(
                "Min Allocations should always be set to 0",
                actualRequest.getAdaptiveAllocationsSettings().getMinNumberOfAllocations(),
                equalTo(0)
            );
            assertThat(actualRequest.getAdaptiveAllocationsSettings().getMaxNumberOfAllocations(), equalTo(9));
            assertNull(actualRequest.getNumberOfAllocations());
        }), any());
    }

    public void testWithAdaptiveAllocationsDisabledButPresent() {
        var request = createRandom(null, new AdaptiveAllocationsSettings(false, 3, 6));
        callFilter(request);
        verify(chain, only()).proceed(any(), eq(StartTrainedModelDeploymentAction.NAME), assertArg(actualRequest -> {
            assertTrue(actualRequest.getAdaptiveAllocationsSettings().getEnabled());
            assertThat(
                "Min Allocations should always be set to 0",
                actualRequest.getAdaptiveAllocationsSettings().getMinNumberOfAllocations(),
                equalTo(0)
            );
            assertThat(actualRequest.getAdaptiveAllocationsSettings().getMaxNumberOfAllocations(), equalTo(6));
            assertNull(actualRequest.getNumberOfAllocations());
        }), any());
    }

    public void testWithAdaptiveAllocationsAbsentWithNumAllocationsSet() {
        var request = createRandom(5, null);
        callFilter(request);
        verify(chain, only()).proceed(any(), eq(StartTrainedModelDeploymentAction.NAME), assertArg(actualRequest -> {
            assertTrue(actualRequest.getAdaptiveAllocationsSettings().getEnabled());
            assertThat(actualRequest.getAdaptiveAllocationsSettings().getMinNumberOfAllocations(), equalTo(0));
            assertThat(actualRequest.getAdaptiveAllocationsSettings().getMaxNumberOfAllocations(), equalTo(5));
            assertNull(actualRequest.getNumberOfAllocations());
        }), any());
    }

    public void testWithAdaptiveAllocationsAbsentWithNoNumAllocations() {
        var request = createRandom(null, null);
        callFilter(request);
        verify(chain, only()).proceed(any(), eq(StartTrainedModelDeploymentAction.NAME), assertArg(actualRequest -> {
            assertTrue(actualRequest.getAdaptiveAllocationsSettings().getEnabled());
            assertThat(actualRequest.getAdaptiveAllocationsSettings().getMinNumberOfAllocations(), equalTo(0));
            assertThat(actualRequest.getAdaptiveAllocationsSettings().getMaxNumberOfAllocations(), equalTo(32));
            assertNull(actualRequest.getNumberOfAllocations());
        }), any());
    }

    public void test() {
        var request = testCase.randomRequest();
        autoEnableFilter = testCase.createFilter();
        callFilter(request);
        testCase.verify(chain);
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            new TestCase(null, null, ProjectType.OBSERVABILITY, true, 0, 32),
            new TestCase(1, null, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE, true, 0, 1),
            new TestCase(5, null, ProjectType.ELASTICSEARCH_SEARCH, true, 0, 5),
            new TestCase(10, null, ProjectType.ELASTICSEARCH_TIMESERIES, true, 0, 10),
            new TestCase(null, new AdaptiveAllocationsSettings(true, 9, 9), ProjectType.ELASTICSEARCH_VECTOR, true, 0, 9),
            new TestCase(1, new AdaptiveAllocationsSettings(false, null, 9), ProjectType.ELASTICSEARCH_VECTOR, true, 0, 9),
            new TestCase(null, new AdaptiveAllocationsSettings(false, 3, 6), ProjectType.OBSERVABILITY, true, 0, 6),
            new TestCase(5, new AdaptiveAllocationsSettings(false, null, null), ProjectType.OBSERVABILITY, true, 0, 5),
            new TestCase(null, null, ProjectType.SECURITY, true, 1, 32),
            new TestCase(1, null, ProjectType.SECURITY, true, 1, 1),
            new TestCase(5, null, ProjectType.SECURITY, true, 1, 5),
            new TestCase(10, null, ProjectType.SECURITY, true, 1, 10),
            new TestCase(null, new AdaptiveAllocationsSettings(true, 9, 9), ProjectType.SECURITY, true, 9, 9),
            new TestCase(null, new AdaptiveAllocationsSettings(false, 3, 6), ProjectType.SECURITY, true, 3, 6),
            new TestCase(5, new AdaptiveAllocationsSettings(false, null, null), ProjectType.SECURITY, true, 1, 5),
            new TestCase(1, new AdaptiveAllocationsSettings(false, null, 9), ProjectType.SECURITY, true, 1, 9),
            new TestCase(1, new AdaptiveAllocationsSettings(true, null, 9), ProjectType.SECURITY, true, 1, 9),
            new TestCase(1, new AdaptiveAllocationsSettings(false, 2, 9), ProjectType.SECURITY, true, 2, 9),
            new TestCase(1, new AdaptiveAllocationsSettings(true, 2, 9), ProjectType.SECURITY, true, 2, 9)
        ).map(TestCase::toArray).toList();
    }

    public record TestCase(
        Integer numAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        ProjectType projectType,
        boolean expectedEnabled,
        int expectedMin,
        int expectedMax
    ) {
        StartTrainedModelDeploymentAction.Request randomRequest() {
            return createRandom(numAllocations, adaptiveAllocationsSettings);
        }

        AutoEnableAdaptiveAllocationsFilter createFilter() {
            return new AutoEnableAdaptiveAllocationsFilter(
                Settings.builder().put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), projectType).build()
            );
        }

        void verify(ActionFilterChain<StartTrainedModelDeploymentAction.Request, CreateTrainedModelAssignmentAction.Response> chain) {
            Mockito.verify(chain, only()).proceed(any(), eq(StartTrainedModelDeploymentAction.NAME), assertArg(actualRequest -> {
                assertEquals(actualRequest.getAdaptiveAllocationsSettings().getEnabled(), expectedEnabled);
                assertThat(actualRequest.getAdaptiveAllocationsSettings().getMinNumberOfAllocations(), equalTo(expectedMin));
                assertThat(actualRequest.getAdaptiveAllocationsSettings().getMaxNumberOfAllocations(), equalTo(expectedMax));
                assertNull(actualRequest.getNumberOfAllocations());
            }), any());
        }

        Object[] toArray() {
            return new Object[] { this };
        }
    }

    private void callFilter(StartTrainedModelDeploymentAction.Request request) {
        autoEnableFilter.apply(mock(), StartTrainedModelDeploymentAction.NAME, request, ActionListener.noop(), chain);
    }

    private static StartTrainedModelDeploymentAction.Request createRandom(
        Integer numAllocations,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        var modelId = randomAlphaOfLength(10);
        var request = new StartTrainedModelDeploymentAction.Request(modelId, modelId);
        if (randomBoolean()) {
            request.setTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setWaitForState(randomFrom(AllocationStatus.State.values()));
        }
        if (randomBoolean()) {
            request.setThreadsPerAllocation(randomFrom(1, 2, 4, 8, 16, 32));
        }
        if (numAllocations != null) {
            request.setNumberOfAllocations(numAllocations);
        }
        if (adaptiveAllocationsSettings != null) {
            request.setAdaptiveAllocationsSettings(adaptiveAllocationsSettings);
        }
        if (randomBoolean()) {
            request.setQueueCapacity(randomIntBetween(1, 100_000));
        }
        if (randomBoolean()) {
            request.setPriority(randomFrom(Priority.values()).toString());
            if ((request.getNumberOfAllocations() != null && request.getNumberOfAllocations() > 1)
                || request.getThreadsPerAllocation() > 1) {
                request.setPriority(Priority.NORMAL.toString());
            }
        }
        return request;
    }

}
