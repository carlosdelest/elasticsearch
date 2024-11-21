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

package co.elastic.elasticsearch.metrics;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Represents an object that provides sampled metrics for reporting
 */
public interface SampledMetricsProvider {

    /** Sink to append backfill usage records. */
    interface BackfillSink {
        default void add(MetricValue sample, Instant timestamp) {
            add(sample, sample.value(), sample.usageMetadata(), timestamp);
        }

        void add(MetricValue sample, long usageValue, Map<String, String> usageMetadata, Instant timestamp);
    }

    /** Strategy for backfilling usage records for sampled metrics. */
    interface BackfillStrategy {
        BackfillStrategy NOOP = new BackfillStrategy() {
            @Override
            public void constant(MetricValue sample, Instant timestamp, BackfillSink sink) {}

            @Override
            public void interpolate(
                Instant curTimestamp,
                MetricValue curSample,
                Instant prevTimestamp,
                MetricValue prevSample,
                Instant timestamp,
                BackfillSink sink
            ) {}
        };

        /** Constant backfill is used for a very limited period if previous samples are not available. */
        void constant(MetricValue sample, Instant timestamp, BackfillSink sink);

        /** Linear interpolation is used to backfill usage records for extended periods based on a previous and the current sample. */
        void interpolate(
            Instant currentTimestamp,
            MetricValue currentSample,
            Instant previousTimestamp,
            MetricValue previousSample,
            Instant timestamp,
            BackfillSink sink
        );
    }

    interface MetricValues extends Iterable<MetricValue> {
        default BackfillStrategy backfillStrategy() {
            return BackfillStrategy.NOOP;
        }
    }

    static MetricValues metricValues(Iterable<MetricValue> iterable, BackfillStrategy backfillStrategy) {
        return new MetricValues() {
            @Override
            public BackfillStrategy backfillStrategy() {
                return backfillStrategy;
            }

            @Override
            public Iterator<MetricValue> iterator() {
                return iterable.iterator();
            }
        };
    }

    /**
     * Returns the current value of the sampled metrics provided by this class.
     * This method may be called at any time - implementations must guarantee thread safety.
     * If the collector has not been able to sample any metrics yet, it must return Optional.empty(), so the caller knows
     * it will need to re-try getting samples for the current timeframe.
     */
    Optional<MetricValues> getMetrics();
}
