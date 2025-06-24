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

package co.elastic.elasticsearch.metering.activitytracking;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

public record Activity(
    Instant lastActivityRecentPeriod,
    Instant firstActivityRecentPeriod,
    Instant lastActivityPreviousPeriod,
    Instant firstActivityPreviousPeriod
) implements Writeable {

    public static Activity EMPTY = new Activity(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);

    public static Snapshot DEFAULT_NOT_ACTIVE = new Snapshot(false, Instant.EPOCH);

    public Activity {
        assert lastActivityRecentPeriod.isBefore(firstActivityRecentPeriod) == false;
        assert firstActivityRecentPeriod.isBefore(lastActivityPreviousPeriod) == false;
        assert lastActivityPreviousPeriod.isBefore(firstActivityPreviousPeriod) == false;
    }

    Activity extendCurrentPeriod(Instant now) {
        var newLastActivity = now.isAfter(lastActivityRecentPeriod) ? now : lastActivityRecentPeriod;
        return new Activity(newLastActivity, firstActivityRecentPeriod, lastActivityPreviousPeriod, firstActivityPreviousPeriod);
    }

    Activity makeNewPeriod(Instant now) {
        return new Activity(now, now, lastActivityRecentPeriod, firstActivityRecentPeriod);
    }

    public boolean isBeforeLastCoolDownExpires(Instant now, Duration coolDown) {
        return now.isAfter(lastActivityRecentPeriod.plus(coolDown)) == false;
    }

    public boolean isEmpty() {
        return this.equals(EMPTY);
    }

    @Nullable
    public Instant firstActivity() {
        if (this.equals(EMPTY)) {
            return null;
        }
        return firstActivityPreviousPeriod.isAfter(Instant.EPOCH) ? firstActivityPreviousPeriod : firstActivityRecentPeriod;
    }

    public record Snapshot(boolean active, Instant lastActivity) {
        public void appendToUsageMetadata(Map<String, String> usageMetadata, String activeFieldName, String lastActivityFieldName) {
            usageMetadata.put(activeFieldName, Boolean.toString(active));
            if (lastActivity.equals(Instant.EPOCH)) {
                // when backfilling, any previous activity timestamp has to be removed
                usageMetadata.remove(lastActivityFieldName);
            } else {
                usageMetadata.put(lastActivityFieldName, lastActivity.toString());
            }
        }
    };

    /**
     * Get the activity snapshot of a time during or after this Activity.
     * If the time is before this Activity, or this Activity is empty, return empty.
     *
     * @param timestamp point in time to check for activity
     * @param coolDown cool down period
     * @return the activity snapshot at timestamp
     */
    public Snapshot activitySnapshot(Instant timestamp, Duration coolDown) {
        return activitySnapshot(timestamp, null, coolDown);
    }

    /**
     * Get the activity snapshot of a time during or after this Activity.
     * If the time is before this Activity, or this Activity is empty, return empty.
     *
     * @param timestamp point in time to check for activity
     * @param previousActivity previous known activity timestamp (before timestamp)
     * @param coolDown cool down period
     * @return the activity snapshot at timestamp
     */
    public Snapshot activitySnapshot(Instant timestamp, @Nullable Instant previousActivity, Duration coolDown) {
        if (isEmpty()) {
            return DEFAULT_NOT_ACTIVE;
        }
        if (lastActivityRecentPeriod.isAfter(timestamp) == false) {
            boolean active = isBeforeLastCoolDownExpires(timestamp, coolDown);
            return new Snapshot(active, lastActivityRecentPeriod);
        }
        // iterate over periods in reverse chronological order
        for (var period : (Iterable<Period>) toPeriods()::iterator) {
            if (timestamp.isAfter(period.last().plus(coolDown))) {
                return new Snapshot(false, period.last());
            }
            if (timestamp.isAfter(period.last())) {
                return new Snapshot(true, period.last());
            }
            if (timestamp.isBefore(period.first()) == false) {
                return new Snapshot(true, timestamp);
            }
        }

        if (previousActivity != null
            && timestamp.isBefore(previousActivity) == false
            && timestamp.isAfter(previousActivity.plus(coolDown)) == false) {
            // timestamp was not covered by activity record, but activity inferred from previous activity
            return new Snapshot(true, previousActivity);
        }

        return DEFAULT_NOT_ACTIVE;
    }

    record Period(Instant last, Instant first) {
        public static Period EMPTY = new Period(Instant.EPOCH, Instant.EPOCH);

        public Period {
            assert last.isBefore(first) == false;
        }
    }

    static Activity fromPeriods(Period recent, Period previous) {
        return new Activity(recent.last, recent.first, previous.last, previous.first);
    }

    /**
     * return periods in reverse chronological order
     */
    private Stream<Period> toPeriods() {
        return Stream.of(
            new Period(lastActivityRecentPeriod, firstActivityRecentPeriod),
            new Period(lastActivityPreviousPeriod, firstActivityPreviousPeriod)
        ).filter(p -> p.last.equals(Instant.EPOCH) == false);
    }

    /**
     * Utility class to merge multiple activities.
     */
    public static class Merger {
        // For merging periods have to be sorted reverse chronologically by end timestamp
        private static final Comparator<Period> PERIOD_MERGE_COMPARATOR = Comparator.comparing(Period::last).reversed();

        private final SortedSet<Period> periods = new TreeSet<>(PERIOD_MERGE_COMPARATOR);

        public void add(Activity activity) {
            if (activity.lastActivityRecentPeriod.equals(Instant.EPOCH) == false) {
                periods.add(new Period(activity.lastActivityRecentPeriod, activity.firstActivityRecentPeriod));
                if (activity.lastActivityPreviousPeriod.equals(Instant.EPOCH) == false) {
                    periods.add(new Period(activity.lastActivityPreviousPeriod, activity.firstActivityPreviousPeriod));
                }
            }
        }

        public Activity merge(Duration coolDown) {
            return mergePeriods(periods, coolDown);
        }

        public static Activity merge(Stream<Activity> activities, Duration coolDown) {
            var activityPeriods = activities.flatMap(Activity::toPeriods).sorted(PERIOD_MERGE_COMPARATOR).toList();
            return mergePeriods(activityPeriods, coolDown);
        }

        private static Activity mergePeriods(Collection<Period> sortedPeriods, Duration coolDown) {
            if (sortedPeriods.isEmpty()) {
                return Activity.EMPTY;
            }

            final List<Period> resultPeriods = new ArrayList<>();

            for (var period : sortedPeriods) {
                if (resultPeriods.isEmpty()) {
                    resultPeriods.add(period);
                    continue;
                }

                var current = resultPeriods.getLast();
                // Because of sort we know that current.last >= period.last
                // Set new current if period is before cool down, else merge with current
                if (period.last.isBefore(current.first.minus(coolDown))) {
                    resultPeriods.add(period);
                } else {
                    var merged = new Period(current.last, min(period.first, current.first));
                    resultPeriods.set(resultPeriods.size() - 1, merged);
                }

                // We only keep two periods, but cannot break until there are three since the next might still merge with current.
                if (resultPeriods.size() >= 3) {
                    break;
                }
            }

            return resultPeriods.size() == 1
                ? Activity.fromPeriods(resultPeriods.get(0), Period.EMPTY)
                : Activity.fromPeriods(resultPeriods.get(0), resultPeriods.get(1));
        }
    }

    private static Instant min(Instant a, Instant b) {
        return a.isBefore(b) ? a : b;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInstant(lastActivityRecentPeriod);
        out.writeInstant(firstActivityRecentPeriod);
        out.writeInstant(lastActivityPreviousPeriod);
        out.writeInstant(firstActivityPreviousPeriod);
    }

    public static Activity readFrom(StreamInput in) throws IOException {
        return new Activity(in.readInstant(), in.readInstant(), in.readInstant(), in.readInstant());
    }
};
