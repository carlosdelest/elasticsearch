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

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.time.Duration;
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
            public Instant current() {
                throw new NoSuchElementException();
            }

            @Override
            public Instant until() {
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

            @Override
            public int size() {
                return 0;
            }

            @Override
            public Timestamps limit(int limit) {
                return this;
            }

            @Override
            public void reset() {}

            @Override
            public String toString() {
                return "EMPTY";
            }
        };

        static Timestamps single(final Instant instant) {
            return new Timestamps() {
                @Override
                public Instant current() {
                    return instant;
                }

                @Override
                public Instant until() {
                    return instant;
                }

                private boolean hasNext = true;

                @Override
                public boolean hasNext() {
                    return hasNext;
                }

                @Override
                public Instant next() {
                    if (hasNext == false) {
                        throw new NoSuchElementException();
                    }
                    hasNext = false;
                    return instant;
                }

                @Override
                public int size() {
                    return 1;
                }

                @Override
                public Timestamps limit(int limit) {
                    return limit == 0 ? EMPTY : this;
                }

                @Override
                public void reset() {
                    hasNext = true;
                }

                @Override
                public String toString() {
                    return instant.toString();
                }
            };
        }

        Timestamps limit(int limit);

        int size();

        Instant current();

        Instant until();

        void reset();
    }

    Optional<Instant> getLatestCommittedTimestamp();

    Timestamps generateSampleTimestamps(Instant current, TimeValue decrement);

    boolean commitUpTo(Instant sampleTimestamp);

    /**
     * Generate decreasing timestamps [current, until) in steps of the given {@code decrement}.
     */
    static Timestamps generateSampleTimestamps(final Instant from, final Instant until, final TimeValue decrement) {
        final var period = Duration.ofNanos(decrement.getNanos());
        final int size = (int) Duration.between(until, from).dividedBy(period);
        return switch (size) {
            case 0 -> Timestamps.EMPTY;
            case 1 -> Timestamps.single(from);
            default -> new Timestamps() {
                Instant current = from;

                @Override
                public Instant current() {
                    return from;
                }

                @Override
                public Instant until() {
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
                    current = current.minus(period);
                    return now;
                }

                @Override
                public int size() {
                    return size;
                }

                @Override
                public Timestamps limit(int limit) {
                    return switch (limit) {
                        case 0 -> Timestamps.EMPTY;
                        case 1 -> Timestamps.single(from);
                        default -> size <= limit ? this : generateSampleTimestamps(from, from.minus(period.multipliedBy(limit)), decrement);
                    };
                }

                @Override
                public void reset() {
                    current = from;
                }

                @Override
                public String toString() {
                    return Strings.format("%s to %s", from, until);
                }
            };
        };
    }
}
