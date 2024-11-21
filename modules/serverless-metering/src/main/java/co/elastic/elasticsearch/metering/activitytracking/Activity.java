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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public record Activity(
    Instant lastActivityRecentPeriod,
    Instant firstActivityRecentPeriod,
    Instant lastActivityPreviousPeriod,
    Instant firstActivityPreviousPeriod
) implements Writeable {

    public static Activity EMPTY = new Activity(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);

    public static ActiveInfo DEFAULT_NOT_ACTIVE = new Activity.ActiveInfo(false, Instant.EPOCH);

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

    public boolean isActive(Instant now, Duration coolDown) {
        return lastActivityRecentPeriod.plus(coolDown).isAfter(now);
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

    public record ActiveInfo(boolean active, Instant lastActivityTime) {};

    /**
     * Show the activity state of a time during or after this Activity.
     * If the time is before this Activity, or this Activity is empty, return empty.
     *
     * @param timestamp point in time to check for activity
     * @param coolDown cool down period
     * @return the activity state at timestamp
     */
    public Optional<ActiveInfo> wasActive(Instant timestamp, Duration coolDown) {
        var periods = toPeriods().toList();
        // iterate over periods in reverse chronological order
        for (var period : periods) {
            if (timestamp.isAfter(period.last().plus(coolDown))) {
                return Optional.of(new ActiveInfo(false, period.last()));
            }
            if (timestamp.isAfter(period.last())) {
                return Optional.of(new ActiveInfo(true, period.last()));
            }
            if (timestamp.isBefore(period.first()) == false) {
                return Optional.of(new ActiveInfo(true, timestamp));
            }
        }
        return Optional.empty();
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

    public static Activity merge(Stream<Activity> activities, Duration coolDown) {

        // Periods sorted reverse chronologically by end timestamp
        final List<Period> activityPeriods = activities.flatMap(Activity::toPeriods)
            .sorted(Comparator.comparing(Period::last).reversed())
            .toList();

        if (activityPeriods.isEmpty()) {
            return Activity.EMPTY;
        }

        final List<Period> resultPeriods = new ArrayList<>();
        resultPeriods.add(activityPeriods.getFirst());

        for (int i = 1; i < activityPeriods.size(); ++i) {
            var current = resultPeriods.getLast();
            var period = activityPeriods.get(i);

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
