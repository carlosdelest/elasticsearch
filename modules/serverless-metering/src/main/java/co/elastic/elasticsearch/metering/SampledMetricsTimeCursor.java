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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metrics.SampledMetricsCollector;

import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

/**
 * An object that keeps track of the latest timestamp for which sampled metrics from {@link SampledMetricsCollector}s have been
 * successfully transmitted.
 */
public interface SampledMetricsTimeCursor {
    Optional<Instant> getLatestCommittedTimestamp();

    Collection<Instant> generateSampleTimestamps(Instant current, TimeValue decrement, int limit);

    boolean commitUpTo(Instant sampleTimestamp);

    /**
     * Generate decreasing timestamps [current, until) in steps of the given {@code decrement}.
     * At most {@code limit} timestamps are returned starting from {@code current}.
     */
    static Collection<Instant> generateSampleTimestamps(Instant current, Instant until, TimeValue decrement, int limit) {
        var timestamps = new ArrayList<Instant>();
        var decrementInNanos = decrement.getNanos();
        for (int i = 0; i < limit && until.isBefore(current); ++i) {
            timestamps.add(current);
            current = current.minusNanos(decrementInNanos);
        }
        return timestamps;
    }
}
