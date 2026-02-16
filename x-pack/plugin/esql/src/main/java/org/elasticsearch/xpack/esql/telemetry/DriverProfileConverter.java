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
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.telemetry.tracing.QueryTraceContext;
import org.elasticsearch.telemetry.tracing.QueryTraceResults;
import org.elasticsearch.telemetry.tracing.QueryTraceSpan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting DriverProfile data into QueryTraceSpan structures
 * and enriching QueryTraceResults with execution-phase profiling data.
 *
 * <p>This converter bridges the gap between the compute layer (DriverProfile)
 * and the telemetry layer (QueryTraceSpan), enabling a unified trace view that
 * spans both query planning and execution phases.
 *
 * <p>Usage:
 * <pre>{@code
 * QueryTraceResults planningTraces = tracer.getResults();
 * QueryTraceResults enrichedTraces = DriverProfileConverter.enrichWithDriverProfiles(
 *     planningTraces,
 *     completionInfo,
 *     tracer.getTraceId()
 * );
 * }</pre>
 */
public final class DriverProfileConverter {

    // Private constructor to prevent instantiation
    private DriverProfileConverter() {}

    /**
     * Enriches existing trace results with driver profile data.
     * Creates spans for each driver and its operators, merging them
     * into the existing trace tree as children of the root span.
     *
     * @param existingTraces  the planning-phase traces from QueryTracer
     * @param completionInfo  the driver completion info containing profiles
     * @param traceId         the trace ID to use for new trace results
     * @return enriched trace results with driver spans merged in, or original traces if no profiles available
     */
    public static QueryTraceResults enrichWithDriverProfiles(
        QueryTraceResults existingTraces,
        DriverCompletionInfo completionInfo,
        String traceId
    ) {
        // Handle null/empty driver profiles
        if (completionInfo == null || completionInfo.driverProfiles().isEmpty()) {
            return existingTraces;
        }

        // If no existing traces, create new trace results from profiles only
        if (existingTraces == null) {
            return createTraceResultsFromProfiles(completionInfo, traceId);
        }

        // Merge driver spans into existing traces
        return mergeDriverSpansIntoTraces(existingTraces, completionInfo);
    }

    /**
     * Creates trace results from driver profiles when no existing traces are available.
     *
     * @param completionInfo the driver completion info containing profiles
     * @param traceId        the trace ID to use
     * @return new trace results containing only driver spans
     */
    private static QueryTraceResults createTraceResultsFromProfiles(DriverCompletionInfo completionInfo, String traceId) {
        List<QueryTraceSpan> driverSpans = new ArrayList<>();
        long maxEndTime = 0;

        for (DriverProfile profile : completionInfo.driverProfiles()) {
            QueryTraceSpan driverSpan = convertDriverProfile(profile);
            driverSpans.add(driverSpan);
            maxEndTime = Math.max(maxEndTime, driverSpan.getEndTimeNanos());
        }

        // Calculate total duration from the earliest start to latest end
        long minStartTime = driverSpans.stream().mapToLong(QueryTraceSpan::getStartTimeNanos).min().orElse(0);
        long totalDuration = maxEndTime - minStartTime;

        return new QueryTraceResults(traceId, driverSpans, totalDuration);
    }

    /**
     * Merges driver spans into existing trace results as children of the root span.
     *
     * @param existingTraces the existing planning traces
     * @param completionInfo the driver completion info containing profiles
     * @return new trace results with driver spans merged in
     */
    private static QueryTraceResults mergeDriverSpansIntoTraces(
        QueryTraceResults existingTraces,
        DriverCompletionInfo completionInfo
    ) {
        QueryTraceSpan rootSpan = existingTraces.rootSpan();
        if (rootSpan == null) {
            // No root span to attach to, create standalone trace
            return createTraceResultsFromProfiles(completionInfo, existingTraces.getTraceId());
        }

        // Convert all driver profiles to spans and add as children of root
        long maxEndTime = rootSpan.getEndTimeNanos();
        for (DriverProfile profile : completionInfo.driverProfiles()) {
            QueryTraceSpan driverSpan = convertDriverProfile(profile);
            rootSpan.addChild(driverSpan);
            maxEndTime = Math.max(maxEndTime, driverSpan.getEndTimeNanos());
        }

        // Recalculate total duration to include execution time
        long totalDuration = maxEndTime - rootSpan.getStartTimeNanos();

        return new QueryTraceResults(existingTraces.getTraceId(), rootSpan, totalDuration);
    }

    /**
     * Converts a single DriverProfile into a QueryTraceSpan hierarchy
     * with child spans for each operator.
     *
     * @param profile the driver profile to convert
     * @return a span representing the driver with operator child spans
     */
    public static QueryTraceSpan convertDriverProfile(DriverProfile profile) {
        // Generate span ID
        String spanId = QueryTraceContext.generateSpanId();

        // TODO Timestamps in profiler are actually in nanoseconds already, we should change the name
        long startTimeNanos = profile.startMillis();
        long endTimeNanos = startTimeNanos + profile.tookNanos();

        // Create driver span with operation name "driver.{description}"
        String operationName = "driver." + profile.description();
        QueryTraceSpan driverSpan = new QueryTraceSpan(
            spanId,
            operationName,
            startTimeNanos,
            endTimeNanos,
            Collections.emptyMap(),
            new ArrayList<>()
        );

        // Add driver-level attributes
        driverSpan.setAttribute("cluster.name", profile.clusterName());
        driverSpan.setAttribute("node.name", profile.nodeName());
        driverSpan.setAttribute("driver.phase", profile.description());
        driverSpan.setAttribute("driver.cpu_nanos", profile.cpuNanos());
        driverSpan.setAttribute("driver.iterations", profile.iterations());
        driverSpan.setAttribute("driver.took_nanos", profile.tookNanos());

        // Calculate and add aggregate metrics
        long totalDocsFound = profile.operators().stream().mapToLong(OperatorStatus::documentsFound).sum();
        long totalValuesLoaded = profile.operators().stream().mapToLong(OperatorStatus::valuesLoaded).sum();
        driverSpan.setAttribute("driver.documents_found", totalDocsFound);
        driverSpan.setAttribute("driver.values_loaded", totalValuesLoaded);

        // Convert operators to child spans
        convertOperators(profile, driverSpan, startTimeNanos);

        // Convert sleep data to attributes
        convertDriverSleeps(profile.sleeps(), driverSpan);

        return driverSpan;
    }

    /**
     * Converts operator statuses to child spans of the driver span.
     * Since operator timing is not tracked individually, durations are estimated
     * by equally distributing the driver's total duration across all operators.
     *
     * @param profile    the driver profile containing operators
     * @param driverSpan the parent driver span to add operator children to
     * @param driverStartNanos the driver's start time in nanoseconds
     */
    private static void convertOperators(DriverProfile profile, QueryTraceSpan driverSpan, long driverStartNanos) {
        List<OperatorStatus> operators = profile.operators();
        if (operators == null || operators.isEmpty()) {
            return;
        }

        // Calculate estimated operator duration (equal distribution)
        long operatorDurationNanos = profile.tookNanos() / operators.size();
        long currentStartNanos = driverStartNanos;

        for (int i = 0; i < operators.size(); i++) {
            OperatorStatus opStatus = operators.get(i);
            long opEndNanos = currentStartNanos + operatorDurationNanos;

            // Generate span ID for operator
            String opSpanId = QueryTraceContext.generateSpanId();

            // Create operator span with operation name "operator.{operatorName}"
            String operationName = "operator." + opStatus.operator();
            QueryTraceSpan opSpan = new QueryTraceSpan(
                opSpanId,
                operationName,
                currentStartNanos,
                opEndNanos,
                Collections.emptyMap(),
                Collections.emptyList()
            );

            // Add operator attributes
            opSpan.setAttribute("operator.name", opStatus.operator());
            opSpan.setAttribute("operator.position", (long) i);
            opSpan.setAttribute("operator.documents_found", opStatus.documentsFound());
            opSpan.setAttribute("operator.values_loaded", opStatus.valuesLoaded());

            // Add status if available
            if (opStatus.status() != null) {
                opSpan.setAttribute("operator.status", opStatus.status().toString());
            }

            // Add note about estimated timing
            opSpan.setAttribute("timing.estimated", true);

            // Add operator span as child of driver span
            driverSpan.addChild(opSpan);

            currentStartNanos = opEndNanos;
        }
    }

    /**
     * Converts driver sleep data to attributes on the driver span.
     * Sleep events are aggregated by reason (counts) and samples of
     * first/last sleep events are included.
     *
     * @param sleeps     the driver sleep data
     * @param driverSpan the driver span to add sleep attributes to
     */
    private static void convertDriverSleeps(DriverSleeps sleeps, QueryTraceSpan driverSpan) {
        if (sleeps == null || sleeps.counts().isEmpty()) {
            return;
        }

        // Add sleep counts by reason
        for (Map.Entry<String, Long> entry : sleeps.counts().entrySet()) {
            String attributeKey = "sleep.count." + entry.getKey();
            driverSpan.setAttribute(attributeKey, entry.getValue());
        }

        // Add first sleep sample if available
        if (sleeps.first().isEmpty() == false) {
            DriverSleeps.Sleep firstSleep = sleeps.first().get(0);
            driverSpan.setAttribute("sleep.first.reason", firstSleep.reason());
            driverSpan.setAttribute("sleep.first.thread_name", firstSleep.threadName());
            driverSpan.setAttribute("sleep.first.sleep_millis", firstSleep.sleep());
            if (firstSleep.wake() > 0) {
                driverSpan.setAttribute("sleep.first.wake_millis", firstSleep.wake());
            }
        }

        // Add last sleep sample if available
        if (sleeps.last().isEmpty() == false) {
            DriverSleeps.Sleep lastSleep = sleeps.last().get(sleeps.last().size() - 1);
            driverSpan.setAttribute("sleep.last.reason", lastSleep.reason());
            driverSpan.setAttribute("sleep.last.thread_name", lastSleep.threadName());
            driverSpan.setAttribute("sleep.last.sleep_millis", lastSleep.sleep());
            if (lastSleep.wake() > 0) {
                driverSpan.setAttribute("sleep.last.wake_millis", lastSleep.wake());
            }
        }
    }
}
