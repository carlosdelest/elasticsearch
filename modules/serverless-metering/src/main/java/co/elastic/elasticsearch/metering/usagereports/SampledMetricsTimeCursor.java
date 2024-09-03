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

package co.elastic.elasticsearch.metering.usagereports;

import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An object that keeps track of the latest timestamp for which sampled metrics from {@link SampledMetricsProvider}s have been
 * successfully transmitted.
 */
public interface SampledMetricsTimeCursor {

    interface Timestamps extends Iterator<Instant> {
        Timestamps EMPTY = new Timestamps() {
            @Override
            public Instant last() {
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Instant next() {
                throw new NoSuchElementException();
            }
        };

        static Timestamps single(Instant instant) {
            return new Timestamps() {
                @Override
                public Instant last() {
                    return instant;
                }

                private Instant value = instant;

                @Override
                public boolean hasNext() {
                    return value != null;
                }

                @Override
                public Instant next() {
                    if (value == null) {
                        throw new NoSuchElementException();
                    }
                    final var res = value;
                    value = null;
                    return res;
                }
            };
        }

        Instant last();
    }

    Optional<Instant> getLatestCommittedTimestamp();

    Timestamps generateSampleTimestamps(Instant current, TimeValue decrement);

    boolean commitUpTo(Instant sampleTimestamp);

    /**
     * Generate decreasing timestamps [current, until) in steps of the given {@code decrement}.
     * At most {@code limit} timestamps are returned starting from {@code current}.
     */
    static Timestamps generateSampleTimestamps(Instant from, Instant until, TimeValue decrement) {
        final var decrementInNanos = decrement.getNanos();
        return new Timestamps() {
            Instant current = from;

            @Override
            public Instant last() {
                return until;
            }

            @Override
            public boolean hasNext() {
                return until.isBefore(current);
            }

            @Override
            public Instant next() {
                if (until.isBefore(current) == false) {
                    throw new NoSuchElementException();
                }
                final var now = current;
                current = current.minusNanos(decrementInNanos);
                return now;
            }
        };
    }
}
