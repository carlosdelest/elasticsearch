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

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ActivityTests extends ESTestCase {

    public static final Duration COOL_DOWN = Duration.ofMinutes(15);

    public void testExtendToNow() {
        Instant first = Instant.now();
        Instant last = first.plusSeconds(100);
        var activity = new Activity(last, first, Instant.EPOCH, Instant.EPOCH);

        // don't update if now is before last
        Instant nowInPast = last.minusSeconds(50);
        assertEquals(new Activity(last, first, Instant.EPOCH, Instant.EPOCH), activity.extendCurrentPeriod(nowInPast));

        Instant now = last.plusSeconds(100);
        assertEquals(new Activity(now, first, Instant.EPOCH, Instant.EPOCH), activity.extendCurrentPeriod(now));
    }

    public void testMergeSingleActivity() {
        Activity activity = randomActivity();
        Activity merged = Activity.merge(Stream.of(activity), COOL_DOWN);
        assertEquals(activity, merged);
    }

    public void testContainedActivityMerged() {
        var times = new TimeSequenceHelper(Instant.now());
        var aFirst2 = times.current;
        var bFirst2 = times.addRand();
        var bLast2 = times.addRand();
        var aLast2 = times.addRand();

        Activity activityA = new Activity(aLast2, aFirst2, Instant.EPOCH, Instant.EPOCH);
        Activity activityB = new Activity(bLast2, bFirst2, Instant.EPOCH, Instant.EPOCH);

        Activity merged = Activity.merge(shuffle(List.of(activityA, activityB)).stream(), COOL_DOWN);
        assertEquals(new Activity(aLast2, aFirst2, Instant.EPOCH, Instant.EPOCH), merged);
    }

    public void testMergeInterleaveNotCombine() {
        // Sequence of events:
        // (aFirst1, aLast1) | >15m | (bFirst1, bLast1) | >15m | (aFirst2, aLast2) | >15m | (bFirst2, bLast2)

        var times = new TimeSequenceHelper(Instant.now());
        var aFirst1 = times.current;
        var aLast1 = times.addRand();
        var bFirst1 = times.addRandMoreThanCoolDown();
        var bLast1 = times.addRand();
        var aFirst2 = times.addRandMoreThanCoolDown();
        var aLast2 = times.addRand();
        var bFirst2 = times.addRandMoreThanCoolDown();
        var bLast2 = times.addRand();

        var activityA = new Activity(aLast2, aFirst2, aLast1, aFirst1);
        var activityB = new Activity(bLast2, bFirst2, bLast1, bFirst1);

        // Ranges are separated by cool down period so should not merge
        Activity merged = Activity.merge(shuffle(List.of(activityA, activityB)).stream(), COOL_DOWN);
        assertEquals(new Activity(bLast2, bFirst2, aLast2, aFirst2), merged);
    }

    public void testMergeInterleaveCombineIntoSinglePeriod() {
        // Sequence of events:
        // (aFirst1, aLast1) | <15m | (bFirst1, bLast1) | <15m | (aFirst2, aLast2) | <15m | (bFirst2, bLast2)

        var times = new TimeSequenceHelper(Instant.now());
        var aFirst1 = times.current;
        var aLast1 = times.addRand();
        var bFirst1 = times.addRandLessThanCoolDown();
        var bLast1 = times.addRand();
        var aFirst2 = times.addRandLessThanCoolDown();
        var aLast2 = times.addRand();
        var bFirst2 = times.addRandLessThanCoolDown();
        var bLast2 = times.addRand();

        var activityA = new Activity(aLast2, aFirst2, aLast1, aFirst1);
        var activityB = new Activity(bLast2, bFirst2, bLast1, bFirst1);

        // Ranges are not separated by cool down so are all merged into a single period
        Activity merged = Activity.merge(shuffle(List.of(activityA, activityB)).stream(), COOL_DOWN);
        assertEquals(new Activity(bLast2, aFirst1, Instant.EPOCH, Instant.EPOCH), merged);
    }

    public void testMergeInterleaveCombineIntoTwoPeriods() {
        // Sequence of events:
        // (aFirst1, aLast1) | <15m | (bFirst1, bLast1) | >15m | (aFirst2, aLast2) | <15m | (bFirst2, bLast2)

        var times = new TimeSequenceHelper(Instant.now());
        var aFirst1 = times.current;
        var aLast1 = times.addRand();
        var bFirst1 = times.addRandLessThanCoolDown();
        var bLast1 = times.addRand();

        var aFirst2 = times.addRandMoreThanCoolDown();
        var aLast2 = times.addRand();
        var bFirst2 = times.addRandLessThanCoolDown();
        var bLast2 = times.addRand();

        var activityA = new Activity(aLast2, aFirst2, aLast1, aFirst1);
        var activityB = new Activity(bLast2, bFirst2, bLast1, bFirst1);

        // Ranges are not separated by cool down so are all merged into a single period
        Activity merged = Activity.merge(shuffle(List.of(activityA, activityB)).stream(), COOL_DOWN);
        assertEquals(new Activity(bLast2, aFirst2, bLast1, aFirst1), merged);
    }

    public static <U> List<U> shuffle(List<U> input) {
        var result = new ArrayList<>(input);
        Collections.shuffle(result, random());
        return result;
    }

    private static Duration randomDuration(Duration min, Duration max) {
        return Duration.ofMillis(randomTimeValue((int) min.toMillis(), (int) max.toMillis(), TimeUnit.MILLISECONDS).millis());
    }

    private static Duration randomDurationOverCoolDown() {
        return randomDuration(COOL_DOWN.plusMillis(1), Duration.ofMinutes(100));
    }

    private static Duration randomDurationUnderCoolDown() {
        // Include COOL_DOWN frequently, so that boundary is tested
        return randomBoolean() ? COOL_DOWN : randomDuration(Duration.ofMillis(1), COOL_DOWN);
    }

    static class TimeSequenceHelper {
        Instant current;

        TimeSequenceHelper(Instant start) {
            current = start;
        }

        public Instant addOffset(Duration offset) {
            current = current.plus(offset);
            return current;
        }

        public Instant subtractOffset(Duration offset) {
            return addOffset(offset.negated());
        }

        public Instant addRand() {
            return addOffset(randomDuration(Duration.ofMinutes(1), Duration.ofMinutes(100)));
        }

        public Instant addRandMoreThanCoolDown() {
            return addOffset(randomDurationOverCoolDown());
        }

        public Instant addRandLessThanCoolDown() {
            return addOffset(randomDurationUnderCoolDown());
        }

        public Instant subtractRand() {
            return subtractOffset(randomDuration(Duration.ofMinutes(1), Duration.ofMinutes(100)));
        }

        public Instant subtractRandMoreThanCoolDown() {
            return subtractOffset(randomDurationOverCoolDown());
        }
    }

    public static Activity randomActivity() {
        return rarely() ? Activity.EMPTY : randomActivityNotEmpty();
    }

    public static Activity randomActivityNotEmpty() {
        return randomBoolean() ? randomActivityNotActive() : randomActivityActive(Duration.ZERO);
    }

    public static Activity randomActivityNotActive() {
        return randomActivity(randomDurationOverCoolDown());
    }

    public static Activity randomActivityActive(Duration minTimeStillActive) {
        var timeToLastActivity = randomDuration(Duration.ofMillis(1), COOL_DOWN.minus(minTimeStillActive));
        return randomActivity(timeToLastActivity);
    }

    public static Activity randomActivity(Duration timeToLastActivity) {
        var now = Instant.now();
        var times = new TimeSequenceHelper(now);
        var last2 = times.subtractOffset(timeToLastActivity);
        var first2 = times.subtractRand();
        var last1 = times.subtractRandMoreThanCoolDown();
        var first1 = times.subtractRand();
        return randomBoolean() ? new Activity(last2, first2, Instant.EPOCH, Instant.EPOCH) : new Activity(last2, first2, last1, first1);
    }
}
