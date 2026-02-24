/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.core.TimeValue;

import java.util.Objects;

public class TimeSpanMarker {
    private TimeSpan timeSpan;
    private transient TimeSpan.Builder timeSpanBuilder;

    private final String name;
    private final boolean allowMultipleCalls;

    public TimeSpanMarker(String name, boolean allowMultipleCalls, TimeSpan timeSpan) {
        this.name = name;
        this.allowMultipleCalls = allowMultipleCalls;
        this.timeSpan = timeSpan;
    }

    public String name() {
        return name;
    }

    public TimeSpan timeSpan() {
        return timeSpan;
    }

    // visible for testing
    public void timeSpan(TimeSpan timeSpan) {
        this.timeSpan = timeSpan;
    }

    public void start() {
        assert allowMultipleCalls || timeSpanBuilder == null : "start() should only be called once for " + name;
        if (timeSpanBuilder == null) {
            timeSpanBuilder = TimeSpan.start();
        }
    }

    public void stop() {
        assert timeSpanBuilder != null : "start() should have been called for " + name;
        assert allowMultipleCalls || timeSpan == null : "start() should only be called once for " + name;
        timeSpan = timeSpanBuilder.stop();
    }

    public TimeValue timeTook() {
        return timeSpan == null ? null : timeSpan.toTimeValue();
    }

    public TimeValue timeSinceStarted() {
        return timeSpanBuilder != null ? timeSpanBuilder.stop().toTimeValue() : TimeValue.ZERO;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TimeSpanMarker that = (TimeSpanMarker) o;
        return allowMultipleCalls == that.allowMultipleCalls && Objects.equals(timeSpan, that.timeSpan) && Objects.equals(name, that.name);
        // Don't consider timeStampBuilders for equality
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeSpan, name, allowMultipleCalls);
    }

    @Override
    public String toString() {
        return "TimeSpanMarker{" + "name='" + name + '\'' + ", timeSpan=" + timeSpan + ", allowMultipleCalls=" + allowMultipleCalls + '}';
    }
}
