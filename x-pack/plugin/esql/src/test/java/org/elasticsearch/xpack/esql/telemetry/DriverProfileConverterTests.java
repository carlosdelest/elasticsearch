/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.telemetry.tracing.QueryTraceResults;
import org.elasticsearch.telemetry.tracing.QueryTraceSpan;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DriverProfileConverterTests extends ESTestCase {

    /**
     * Test basic conversion of a single driver profile to a trace span.
     */
    public void testConvertSingleDriverProfile() {
        // Create a simple driver profile
        DriverProfile profile = createDriverProfile(
            "data",
            "test-cluster",
            "test-node",
            1000L,
            2000L,
            500_000_000L,
            300_000_000L,
            100L,
            List.of(),
            DriverSleeps.empty()
        );

        QueryTraceSpan span = DriverProfileConverter.convertDriverProfile(profile);

        assertNotNull(span);
        assertEquals("driver.data", span.getOperationName());
        assertEquals(1000L * 1_000_000L, span.getStartTimeNanos());
        assertEquals(1000L * 1_000_000L + 500_000_000L, span.getEndTimeNanos());
        assertEquals(500_000_000L, span.getDurationNanos());

        // Verify driver attributes
        assertEquals("test-cluster", span.getAttributes().get("cluster.name"));
        assertEquals("test-node", span.getAttributes().get("node.name"));
        assertEquals("data", span.getAttributes().get("driver.phase"));
        assertEquals(300_000_000L, span.getAttributes().get("driver.cpu_nanos"));
        assertEquals(100L, span.getAttributes().get("driver.iterations"));
        assertEquals(500_000_000L, span.getAttributes().get("driver.took_nanos"));
        assertEquals(0L, span.getAttributes().get("driver.documents_found"));
        assertEquals(0L, span.getAttributes().get("driver.values_loaded"));
    }

    /**
     * Test conversion of operators to child spans.
     */
    public void testConvertOperatorsToChildSpans() {
        // Create operator statuses
        List<OperatorStatus> operators = new ArrayList<>();
        operators.add(new OperatorStatus("LuceneSourceOperator", createMockStatus(100L, 500L)));
        operators.add(new OperatorStatus("ValuesSourceReaderOperator", createMockStatus(50L, 200L)));
        operators.add(new OperatorStatus("ProjectOperator", createMockStatus(0L, 0L)));

        DriverProfile profile = createDriverProfile(
            "data",
            "test-cluster",
            "test-node",
            1000L,
            2000L,
            900_000_000L,
            600_000_000L,
            150L,
            operators,
            DriverSleeps.empty()
        );

        QueryTraceSpan span = DriverProfileConverter.convertDriverProfile(profile);

        // Verify child spans were created
        assertEquals(3, span.getChildren().size());

        // Verify first operator
        QueryTraceSpan op1 = span.getChildren().get(0);
        assertEquals("operator.LuceneSourceOperator", op1.getOperationName());
        assertEquals("LuceneSourceOperator", op1.getAttributes().get("operator.name"));
        assertEquals(0L, op1.getAttributes().get("operator.position"));
        assertEquals(100L, op1.getAttributes().get("operator.documents_found"));
        assertEquals(500L, op1.getAttributes().get("operator.values_loaded"));
        assertEquals(true, op1.getAttributes().get("timing.estimated"));

        // Verify second operator
        QueryTraceSpan op2 = span.getChildren().get(1);
        assertEquals("operator.ValuesSourceReaderOperator", op2.getOperationName());
        assertEquals(1L, op2.getAttributes().get("operator.position"));
        assertEquals(50L, op2.getAttributes().get("operator.documents_found"));
        assertEquals(200L, op2.getAttributes().get("operator.values_loaded"));

        // Verify third operator
        QueryTraceSpan op3 = span.getChildren().get(2);
        assertEquals("operator.ProjectOperator", op3.getOperationName());
        assertEquals(2L, op3.getAttributes().get("operator.position"));
        assertEquals(0L, op3.getAttributes().get("operator.documents_found"));
        assertEquals(0L, op3.getAttributes().get("operator.values_loaded"));

        // Verify aggregate metrics on driver span
        assertEquals(150L, span.getAttributes().get("driver.documents_found"));
        assertEquals(700L, span.getAttributes().get("driver.values_loaded"));
    }

    /**
     * Test conversion of driver sleep data to attributes.
     */
    public void testConvertDriverSleeps() {
        // Create sleep data
        DriverSleeps sleeps = new DriverSleeps(
            Map.of("waiting_for_data", 5L, "waiting_for_memory", 3L),
            List.of(new DriverSleeps.Sleep("waiting_for_data", "thread-1", 1000L, 1050L)),
            List.of(new DriverSleeps.Sleep("waiting_for_memory", "thread-2", 2000L, 2100L))
        );

        DriverProfile profile = createDriverProfile(
            "data",
            "test-cluster",
            "test-node",
            1000L,
            3000L,
            1_000_000_000L,
            800_000_000L,
            200L,
            List.of(),
            sleeps
        );

        QueryTraceSpan span = DriverProfileConverter.convertDriverProfile(profile);

        // Verify sleep count attributes
        assertEquals(5L, span.getAttributes().get("sleep.count.waiting_for_data"));
        assertEquals(3L, span.getAttributes().get("sleep.count.waiting_for_memory"));

        // Verify first sleep sample
        assertEquals("waiting_for_data", span.getAttributes().get("sleep.first.reason"));
        assertEquals("thread-1", span.getAttributes().get("sleep.first.thread_name"));
        assertEquals(1000L, span.getAttributes().get("sleep.first.sleep_millis"));
        assertEquals(1050L, span.getAttributes().get("sleep.first.wake_millis"));

        // Verify last sleep sample
        assertEquals("waiting_for_memory", span.getAttributes().get("sleep.last.reason"));
        assertEquals("thread-2", span.getAttributes().get("sleep.last.thread_name"));
        assertEquals(2000L, span.getAttributes().get("sleep.last.sleep_millis"));
        assertEquals(2100L, span.getAttributes().get("sleep.last.wake_millis"));
    }

    /**
     * Test enrichment with null driver profiles returns original traces unchanged.
     */
    public void testEnrichWithNullDriverProfiles() {
        String traceId = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
        QueryTraceSpan rootSpan = new QueryTraceSpan("root123", "esql.query", 1000L);
        rootSpan.end(2000L);
        QueryTraceResults existingTraces = new QueryTraceResults(traceId, rootSpan, 1000L);

        // Enrich with null completion info
        QueryTraceResults result = DriverProfileConverter.enrichWithDriverProfiles(existingTraces, null, traceId);

        assertSame(existingTraces, result);
    }

    /**
     * Test enrichment with empty driver profiles returns original traces unchanged.
     */
    public void testEnrichWithEmptyDriverProfiles() {
        String traceId = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
        QueryTraceSpan rootSpan = new QueryTraceSpan("root123", "esql.query", 1000L);
        rootSpan.end(2000L);
        QueryTraceResults existingTraces = new QueryTraceResults(traceId, rootSpan, 1000L);

        DriverCompletionInfo emptyInfo = new DriverCompletionInfo(0L, 0L, List.of(), List.of());

        QueryTraceResults result = DriverProfileConverter.enrichWithDriverProfiles(existingTraces, emptyInfo, traceId);

        assertSame(existingTraces, result);
    }

    /**
     * Test creating trace results from profiles when no existing traces available.
     */
    public void testCreateTraceResultsFromProfilesOnly() {
        String traceId = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

        DriverProfile profile1 = createDriverProfile(
            "data",
            "cluster1",
            "node1",
            1000L,
            2000L,
            500_000_000L,
            300_000_000L,
            100L,
            List.of(),
            DriverSleeps.empty()
        );

        DriverProfile profile2 = createDriverProfile(
            "final",
            "cluster1",
            "node1",
            2000L,
            3000L,
            300_000_000L,
            200_000_000L,
            50L,
            List.of(),
            DriverSleeps.empty()
        );

        DriverCompletionInfo completionInfo = new DriverCompletionInfo(
            0L,
            0L,
            List.of(profile1, profile2),
            List.of()
        );

        QueryTraceResults result = DriverProfileConverter.enrichWithDriverProfiles(null, completionInfo, traceId);

        assertNotNull(result);
        assertEquals(traceId, result.getTraceId());
        assertEquals(2, result.getSpans().size());

        // Verify first driver span
        QueryTraceSpan span1 = result.getSpans().get(0);
        assertEquals("driver.data", span1.getOperationName());

        // Verify second driver span
        QueryTraceSpan span2 = result.getSpans().get(1);
        assertEquals("driver.final", span2.getOperationName());
    }

    /**
     * Test merging driver spans into existing trace results.
     */
    public void testMergeDriverSpansIntoExistingTraces() {
        String traceId = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

        // Create existing planning traces
        QueryTraceSpan rootSpan = new QueryTraceSpan("root123", "esql.query", 1000_000_000L);
        QueryTraceSpan parseSpan = new QueryTraceSpan("parse123", "esql.parse", 1000_000_000L);
        parseSpan.end(1100_000_000L);
        rootSpan.addChild(parseSpan);
        rootSpan.end(1500_000_000L);

        QueryTraceResults existingTraces = new QueryTraceResults(traceId, rootSpan, 500_000_000L);

        // Create driver profile
        DriverProfile profile = createDriverProfile(
            "data",
            "cluster1",
            "node1",
            1L,
            3L,
            1_000_000_000L,
            800_000_000L,
            150L,
            List.of(),
            DriverSleeps.empty()
        );

        DriverCompletionInfo completionInfo = new DriverCompletionInfo(
            0L,
            0L,
            List.of(profile),
            List.of()
        );

        QueryTraceResults result = DriverProfileConverter.enrichWithDriverProfiles(existingTraces, completionInfo, traceId);

        assertNotNull(result);
        assertEquals(traceId, result.getTraceId());

        // Verify root span has both planning and driver children
        QueryTraceSpan mergedRoot = result.rootSpan();
        assertNotNull(mergedRoot);
        assertEquals(2, mergedRoot.getChildren().size()); // parse span + driver span

        // Verify driver span was added
        QueryTraceSpan driverSpan = mergedRoot.getChildren().get(1);
        assertEquals("driver.data", driverSpan.getOperationName());

        // Verify total duration was updated
        assertTrue(result.getTotalDurationNanos() >= 500_000_000L);
    }

    /**
     * Test handling of empty operators list.
     */
    public void testEmptyOperatorsList() {
        DriverProfile profile = createDriverProfile(
            "data",
            "test-cluster",
            "test-node",
            1000L,
            2000L,
            500_000_000L,
            300_000_000L,
            100L,
            List.of(), // Empty operators
            DriverSleeps.empty()
        );

        QueryTraceSpan span = DriverProfileConverter.convertDriverProfile(profile);

        assertNotNull(span);
        assertEquals(0, span.getChildren().size()); // No operator children
        assertEquals(0L, span.getAttributes().get("driver.documents_found"));
        assertEquals(0L, span.getAttributes().get("driver.values_loaded"));
    }

    /**
     * Test handling of null sleeps data.
     */
    public void testNullSleeps() {
        DriverProfile profile = createDriverProfile(
            "data",
            "test-cluster",
            "test-node",
            1000L,
            2000L,
            500_000_000L,
            300_000_000L,
            100L,
            List.of(),
            null // Null sleeps
        );

        QueryTraceSpan span = DriverProfileConverter.convertDriverProfile(profile);

        assertNotNull(span);
        // Verify no sleep attributes are present
        assertFalse(span.getAttributes().containsKey("sleep.count.waiting_for_data"));
        assertFalse(span.getAttributes().containsKey("sleep.first.reason"));
        assertFalse(span.getAttributes().containsKey("sleep.last.reason"));
    }

    /**
     * Test handling of operator status with null status field.
     */
    public void testOperatorWithNullStatus() {
        List<OperatorStatus> operators = List.of(
            new OperatorStatus("TestOperator", null)
        );

        DriverProfile profile = createDriverProfile(
            "data",
            "test-cluster",
            "test-node",
            1000L,
            2000L,
            500_000_000L,
            300_000_000L,
            100L,
            operators,
            DriverSleeps.empty()
        );

        QueryTraceSpan span = DriverProfileConverter.convertDriverProfile(profile);

        assertEquals(1, span.getChildren().size());
        QueryTraceSpan opSpan = span.getChildren().get(0);
        assertEquals("operator.TestOperator", opSpan.getOperationName());
        // Status should not be present when null
        assertFalse(opSpan.getAttributes().containsKey("operator.status"));
    }

    // Helper methods

    private DriverProfile createDriverProfile(
        String description,
        String clusterName,
        String nodeName,
        long startMillis,
        long stopMillis,
        long tookNanos,
        long cpuNanos,
        long iterations,
        List<OperatorStatus> operators,
        DriverSleeps sleeps
    ) {
        return new DriverProfile(
            description,
            clusterName,
            nodeName,
            startMillis,
            stopMillis,
            tookNanos,
            cpuNanos,
            iterations,
            operators,
            sleeps != null ? sleeps : DriverSleeps.empty()
        );
    }

    private Operator.Status createMockStatus(long documentsFound, long valuesLoaded) {
        return new Operator.Status() {
            @Override
            public String getWriteableName() {
                return "mock";
            }

            @Override
            public long documentsFound() {
                return documentsFound;
            }

            @Override
            public long valuesLoaded() {
                return valuesLoaded;
            }
        };
    }
}
