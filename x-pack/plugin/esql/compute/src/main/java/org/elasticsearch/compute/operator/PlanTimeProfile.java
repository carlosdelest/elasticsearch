/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.TimeSpan;
import org.elasticsearch.common.time.TimeSpanMarker;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Profile information for plan optimization phases.
 * Captures timing information for logical and physical optimization steps,
 * including start and stop timestamps for each phase.
 */
public final class PlanTimeProfile implements Writeable, ToXContentObject {

    public static final String LOGICAL_OPTIMIZATION = "logical_optimization";
    public static final String PHYSICAL_OPTIMIZATION = "physical_optimization";
    public static final String REDUCTION = "reduction";

    private static final TransportVersion PLAN_TIME_PROFILE_TIMESPAN = TransportVersion.fromName("plan_time_profile_timespan");

    /** Time span for logical plan optimization */
    private final TimeSpanMarker logicalOptimizationMarker;
    /** Time span for physical plan optimization */
    private final TimeSpanMarker physicalOptimizationMarker;
    /** Time span for reduction plan building */
    private final TimeSpanMarker reductionMarker;

    /**
     * Creates a new empty profile for production use. Call {@link #logicalOptimization()},
     * {@link #physicalOptimization()}, or {@link #reduction()} to get markers, then
     * use {@link TimeSpanMarker#start()} and {@link TimeSpanMarker#stop()} around each phase.
     */
    public PlanTimeProfile() {
        this(null, null, null);
    }

    /**
     * Creates a profile with pre-existing time spans. Used for deserialization and testing.
     *
     * @param logicalOptimization  time span for logical optimization, or null if not measured
     * @param physicalOptimization time span for physical optimization, or null if not measured
     * @param reduction            time span for reduction plan, or null if not measured
     */
    public PlanTimeProfile(TimeSpan logicalOptimization, TimeSpan physicalOptimization, TimeSpan reduction) {
        this.logicalOptimizationMarker = new TimeSpanMarker(LOGICAL_OPTIMIZATION, true, logicalOptimization);
        this.physicalOptimizationMarker = new TimeSpanMarker(PHYSICAL_OPTIMIZATION, true, physicalOptimization);
        this.reductionMarker = new TimeSpanMarker(REDUCTION, false, reduction);
    }

    public static PlanTimeProfile readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(PLAN_TIME_PROFILE_TIMESPAN)) {
            return new PlanTimeProfile(
                in.readOptionalWriteable(TimeSpan::readFrom),
                in.readOptionalWriteable(TimeSpan::readFrom),
                in.readOptionalWriteable(TimeSpan::readFrom)
            );
        } else {
            // Backwards compat: read old VLong durations, convert to TimeSpan with zero start
            long logical = in.readVLong();
            long physical = in.readVLong();
            long reduction = in.readVLong();
            return new PlanTimeProfile(
                logical > 0 ? new TimeSpan(0, 0, 0, logical) : null,
                physical > 0 ? new TimeSpan(0, 0, 0, physical) : null,
                reduction > 0 ? new TimeSpan(0, 0, 0, reduction) : null
            );
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(PLAN_TIME_PROFILE_TIMESPAN)) {
            out.writeOptionalWriteable(logicalOptimizationMarker.timeSpan());
            out.writeOptionalWriteable(physicalOptimizationMarker.timeSpan());
            out.writeOptionalWriteable(reductionMarker.timeSpan());
        } else {
            // Backwards compat: write durations as VLongs
            out.writeVLong(durationNanos(logicalOptimizationMarker));
            out.writeVLong(durationNanos(physicalOptimizationMarker));
            out.writeVLong(durationNanos(reductionMarker));
        }
    }

    private static long durationNanos(TimeSpanMarker marker) {
        TimeSpan span = marker.timeSpan();
        return span != null ? span.durationInNanos() : 0L;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (TimeSpanMarker marker : timeSpanMarkers()) {
            if (marker.timeSpan() != null) {
                builder.field(marker.name(), marker.timeSpan());
            }
        }
        return builder;
    }

    /**
     * Marker for logical plan optimization timing. Use {@link TimeSpanMarker#start()} and
     * {@link TimeSpanMarker#stop()} to capture the time span.
     */
    public TimeSpanMarker logicalOptimization() {
        return logicalOptimizationMarker;
    }

    /**
     * Marker for physical plan optimization timing. Use {@link TimeSpanMarker#start()} and
     * {@link TimeSpanMarker#stop()} to capture the time span.
     */
    public TimeSpanMarker physicalOptimization() {
        return physicalOptimizationMarker;
    }

    /**
     * Marker for reduction plan building timing. Use {@link TimeSpanMarker#start()} and
     * {@link TimeSpanMarker#stop()} to capture the time span.
     */
    public TimeSpanMarker reduction() {
        return reductionMarker;
    }

    public Collection<TimeSpanMarker> timeSpanMarkers() {
        return List.of(logicalOptimizationMarker, physicalOptimizationMarker, reductionMarker);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PlanTimeProfile) obj;
        return Objects.equals(logicalOptimizationMarker, that.logicalOptimizationMarker)
            && Objects.equals(physicalOptimizationMarker, that.physicalOptimizationMarker)
            && Objects.equals(reductionMarker, that.reductionMarker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalOptimizationMarker, physicalOptimizationMarker, reductionMarker);
    }

    @Override
    public String toString() {
        return "PlanTimeProfile["
            + "logicalOptimization="
            + logicalOptimizationMarker
            + ", physicalOptimization="
            + physicalOptimizationMarker
            + ", reduction="
            + reductionMarker
            + ']';
    }
}
