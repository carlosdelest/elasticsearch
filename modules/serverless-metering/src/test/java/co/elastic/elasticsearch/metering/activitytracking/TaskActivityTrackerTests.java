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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskActivityTrackerTests extends ESTestCase {

    private static final String SEARCH_ACTION = "search action";
    private static final String INDEX_ACTION = "index action";
    private static final String BOTH_ACTION = "both action";
    private static final String NEITHER_ACTION = "neither action";
    private static final Duration coolDown = Duration.ofMinutes(15);

    static ActionTier.Mapper TEST_MAPPER = action -> switch (action) {
        case SEARCH_ACTION -> ActionTier.SEARCH;
        case INDEX_ACTION -> ActionTier.INDEX;
        case BOTH_ACTION -> ActionTier.BOTH;
        case NEITHER_ACTION -> ActionTier.NEITHER;
        default -> throw new IllegalArgumentException(action + " is not an allowed value");
    };

    public void testInternalUserThatIsTracked() {
        String actionTested = randomFrom(SEARCH_ACTION, INDEX_ACTION);
        var start1 = Instant.now();
        var end1 = start1.plus(1, ChronoUnit.SECONDS);
        var clock = createClock(start1, end1);

        var trackedInternalUser = randomFrom(
            InternalUsers.get()
                .stream()
                .filter(iu -> TaskActivityTracker.INTERNAL_USERS_TO_IGNORE.contains(iu) == false)
                .collect(Collectors.toList())
        );

        var tracker = TaskActivityTracker.build(
            clock,
            coolDown,
            randomBoolean(),
            nonOperatorThreadContext(),
            TEST_MAPPER,
            mock(TaskManager.class),
            createSecurityContext(trackedInternalUser)
        );

        var action = randomBoolean() ? actionTested : BOTH_ACTION;
        var task1 = createTask(1, action);
        tracker.onTaskStart(action, task1);
        tracker.onTaskFinish(task1);

        assertEquals(pickActivityForAction(actionTested, tracker), new Activity(end1, start1, Instant.EPOCH, Instant.EPOCH));
    }

    public void testUntrackedInternalUserHasNoEffect() {
        boolean hasSearchRole = randomBoolean();
        ActionTier.Mapper mapper = action -> randomFrom(ActionTier.values());
        var activityTracker = TaskActivityTracker.build(
            Clock.systemUTC(),
            coolDown,
            hasSearchRole,
            nonOperatorThreadContext(),
            mapper,
            mock(TaskManager.class),
            createSecurityContext(randomFrom(TaskActivityTracker.INTERNAL_USERS_TO_IGNORE))
        );

        var task = createTask(1, "fake action");
        activityTracker.onTaskStart("fake action", task);

        assertEquals(activityTracker.getSearchSampleActivity(), Activity.EMPTY);
        assertEquals(activityTracker.getIndexSampleActivity(), Activity.EMPTY);

        activityTracker.onTaskFinish(task);
        assertEquals(activityTracker.getSearchSampleActivity(), Activity.EMPTY);
        assertEquals(activityTracker.getIndexSampleActivity(), Activity.EMPTY);
    }

    public void testOperatorHasNoEffect() {
        boolean hasSearchRole = randomBoolean();
        ActionTier.Mapper mapper = action -> randomFrom(ActionTier.values());
        var threadContext = operatorThreadContext();
        var activityTracker = TaskActivityTracker.build(
            Clock.systemUTC(),
            coolDown,
            hasSearchRole,
            threadContext,
            mapper,
            mock(TaskManager.class)
        );

        var task = createTask(1, "fake action");
        activityTracker.onTaskStart("fake action", task);

        assertEquals(activityTracker.getSearchSampleActivity(), Activity.EMPTY);
        assertEquals(activityTracker.getIndexSampleActivity(), Activity.EMPTY);

        activityTracker.onTaskFinish(task);
        assertEquals(activityTracker.getSearchSampleActivity(), Activity.EMPTY);
        assertEquals(activityTracker.getIndexSampleActivity(), Activity.EMPTY);
    }

    public void testNeitherHasNoEffect() {
        boolean hasSearchRole = randomBoolean();
        ActionTier.Mapper mapper = action -> ActionTier.NEITHER;

        var activityTracker = TaskActivityTracker.build(
            Clock.systemUTC(),
            coolDown,
            hasSearchRole,
            nonOperatorThreadContext(),
            mapper,
            mock(TaskManager.class)
        );

        activityTracker.onTaskStart("fake action", createTask(1, "fake action"));

        assertEquals(activityTracker.getSearchSampleActivity(), Activity.EMPTY);
        assertEquals(activityTracker.getIndexSampleActivity(), Activity.EMPTY);
    }

    public void testSingleTierOneEventSync() {
        boolean hasSearchRole = randomBoolean();
        var threadContext = nonOperatorThreadContext();
        String actionTested = randomFrom(SEARCH_ACTION, INDEX_ACTION);
        var start1 = Instant.now();
        var end1 = start1.plus(1, ChronoUnit.SECONDS);
        var clock = createClock(start1, end1);
        var tracker = TaskActivityTracker.build(clock, coolDown, hasSearchRole, threadContext, TEST_MAPPER, mock(TaskManager.class));

        var action = randomBoolean() ? actionTested : BOTH_ACTION;
        var task1 = createTask(1, action);
        tracker.onTaskStart(action, task1);
        tracker.onTaskFinish(task1);

        assertEquals(pickActivityForAction(actionTested, tracker), new Activity(end1, start1, Instant.EPOCH, Instant.EPOCH));
    }

    // Test that sync actions which are spaced less than cooldown period apart are put in a single period
    public void testSingleTierManySyncEventsSamePeriod() {
        boolean hasSearchRole = randomBoolean();
        var threadContext = nonOperatorThreadContext();
        String actionTested = randomFrom(SEARCH_ACTION, INDEX_ACTION);

        int numActions = between(1, 20);
        List<Instant> times = new ArrayList<>();
        times.add(Instant.now());
        addTimeWithOffset(times, Duration.ofSeconds(1));
        for (int i = 1; i < numActions; ++i) {
            // Difference between action and previous is just below the threshold for making a new period
            addTimeWithOffset(times, Duration.ofSeconds(coolDown.getSeconds()));
            addTimeWithOffset(times, Duration.ofSeconds(1));
        }

        var clock = createClock(times);
        var tracker = TaskActivityTracker.build(clock, coolDown, hasSearchRole, threadContext, TEST_MAPPER, mock(TaskManager.class));

        for (int i = 0; i < numActions; ++i) {
            var action = randomBoolean() ? actionTested : BOTH_ACTION;
            var task = createTask(i, action);
            tracker.onTaskStart(action, task);
            tracker.onTaskFinish(task);
        }

        assertEquals(
            pickActivityForAction(actionTested, tracker),
            new Activity(times.get(times.size() - 1), times.get(0), Instant.EPOCH, Instant.EPOCH)
        );
    }

    // Test that sync actions which are spaced more than cooldown period apart are put in different periods
    public void testSingleTierManySyncEventsDifferentPeriods() {
        boolean hasSearchRole = randomBoolean();
        var threadContext = nonOperatorThreadContext();
        String actionTested = randomFrom(SEARCH_ACTION, INDEX_ACTION);

        int numActions = between(2, 20);
        List<Instant> times = new ArrayList<>();
        times.add(Instant.now());
        addTimeWithOffset(times, Duration.ofSeconds(1));
        for (int i = 1; i < numActions; ++i) {
            // Difference between action and previous is now above threshold
            addTimeWithOffset(times, Duration.ofSeconds(coolDown.getSeconds() + 1));
            addTimeWithOffset(times, Duration.ofSeconds(1));
        }

        var clock = createClock(times);
        var tracker = TaskActivityTracker.build(clock, coolDown, hasSearchRole, threadContext, TEST_MAPPER, mock(TaskManager.class));

        for (int i = 0; i < numActions; ++i) {
            var action = randomBoolean() ? actionTested : BOTH_ACTION;
            var task = createTask(i, action);
            tracker.onTaskStart(action, task);
            tracker.onTaskFinish(task);
        }

        assertEquals(
            pickActivityForAction(actionTested, tracker),
            new Activity(times.get(times.size() - 1), times.get(times.size() - 2), times.get(times.size() - 3), times.get(times.size() - 4))
        );
    }

    public void testSingleTierRepeatedSampleWithAsync() {
        boolean hasSearchRole = randomBoolean();
        var threadContext = nonOperatorThreadContext();
        String actionTested = randomFrom(SEARCH_ACTION, INDEX_ACTION);

        var asyncStart = Instant.now();
        var sample1 = asyncStart.plus(Duration.ofSeconds(coolDown.getSeconds() + 1));
        var sample2 = sample1.plus(Duration.ofSeconds(coolDown.getSeconds() + 1));
        var asyncFinish = sample2.plus(Duration.ofSeconds(coolDown.getSeconds() + 1));
        var sample3 = sample2.plus(Duration.ofSeconds(coolDown.getSeconds() + 1));

        var clock = createClock(asyncStart, sample1, sample2, asyncFinish, sample3);
        var tracker = TaskActivityTracker.build(clock, coolDown, hasSearchRole, threadContext, TEST_MAPPER, mock(TaskManager.class));

        var action = randomBoolean() ? actionTested : BOTH_ACTION;
        var task = createTask(1L, action);
        tracker.onTaskStart(action, task);

        // Each call to sample updates the last timestamp to "now"
        assertEquals(pickActivityForAction(actionTested, tracker), new Activity(sample1, asyncStart, Instant.EPOCH, Instant.EPOCH));
        assertEquals(pickActivityForAction(actionTested, tracker), new Activity(sample2, asyncStart, Instant.EPOCH, Instant.EPOCH));

        tracker.onTaskFinish(task);

        // After finish last timestamp is finish time, not "now"
        assertEquals(pickActivityForAction(actionTested, tracker), new Activity(asyncFinish, asyncStart, Instant.EPOCH, Instant.EPOCH));
    }

    public void testMultipleTierInterleaving() {
        boolean hasSearchRole = randomBoolean();
        var threadContext = nonOperatorThreadContext();

        var searchStart = Instant.now();
        var indexStart = searchStart.plus(Duration.ofSeconds(10));
        var sampleSearch = searchStart.plus(Duration.ofMinutes(5));
        var sampleIndex = indexStart.plus(Duration.ofMinutes(5));
        var searchEnd = searchStart.plus(Duration.ofSeconds(coolDown.getSeconds()));
        var indexEnd = indexStart.plus(Duration.ofSeconds(coolDown.getSeconds()));
        // starts new period
        var bothStart = indexEnd.plus(Duration.ofMinutes(coolDown.getSeconds() + 1));
        var bothSearchSample = bothStart.plus(Duration.ofMinutes(5));
        var bothIndexSample = bothStart.plus(Duration.ofMinutes(6));
        var bothEnd = bothStart.plus(Duration.ofMinutes(10));

        var clock = createClock(
            searchStart,
            indexStart,
            sampleSearch,
            sampleIndex,
            searchEnd,
            indexEnd,
            bothStart,
            bothSearchSample,
            bothIndexSample,
            bothEnd
        );
        var tracker = TaskActivityTracker.build(clock, coolDown, hasSearchRole, threadContext, TEST_MAPPER, mock(TaskManager.class));

        var searchTask = createTask(0, SEARCH_ACTION);
        var indexTask = createTask(1, INDEX_ACTION);

        // Interleave a search and an index task
        tracker.onTaskStart(SEARCH_ACTION, searchTask);
        tracker.onTaskStart(INDEX_ACTION, indexTask);

        assertEquals(tracker.getSearchSampleActivity(), new Activity(sampleSearch, searchStart, Instant.EPOCH, Instant.EPOCH));
        assertEquals(tracker.getIndexSampleActivity(), new Activity(sampleIndex, indexStart, Instant.EPOCH, Instant.EPOCH));

        tracker.onTaskFinish(searchTask);
        tracker.onTaskFinish(indexTask);

        assertEquals(tracker.getSearchSampleActivity(), new Activity(searchEnd, searchStart, Instant.EPOCH, Instant.EPOCH));
        assertEquals(tracker.getIndexSampleActivity(), new Activity(indexEnd, indexStart, Instant.EPOCH, Instant.EPOCH));

        // Now a both task
        var bothTask = createTask(2, BOTH_ACTION);
        tracker.onTaskStart(BOTH_ACTION, bothTask);

        assertEquals(tracker.getSearchSampleActivity(), new Activity(bothSearchSample, bothStart, searchEnd, searchStart));
        assertEquals(tracker.getIndexSampleActivity(), new Activity(bothIndexSample, bothStart, indexEnd, indexStart));

        tracker.onTaskFinish(bothTask);

        // Final sample has same value
        assertEquals(tracker.getSearchSampleActivity(), new Activity(bothEnd, bothStart, searchEnd, searchStart));
        assertEquals(tracker.getIndexSampleActivity(), new Activity(bothEnd, bothStart, indexEnd, indexStart));
    }

    public void testDontStartNewPeriodIfAsyncRunning() {
        boolean hasSearchRole = randomBoolean();
        var threadContext = nonOperatorThreadContext();

        var start1 = Instant.now();
        var start2 = start1.plus(coolDown.getSeconds() + 1, ChronoUnit.SECONDS);
        var sample = start2.plus(1, ChronoUnit.SECONDS);

        var clock = createClock(start1, start2, sample);
        var tracker = TaskActivityTracker.build(clock, coolDown, hasSearchRole, threadContext, TEST_MAPPER, mock(TaskManager.class));

        var testSearchActions = randomBoolean();
        var action1 = testSearchActions ? randomFrom(SEARCH_ACTION, BOTH_ACTION) : randomFrom(INDEX_ACTION, BOTH_ACTION);
        var action2 = testSearchActions ? randomFrom(SEARCH_ACTION, BOTH_ACTION) : randomFrom(INDEX_ACTION, BOTH_ACTION);

        // start task and leave running
        tracker.onTaskStart(action1, createTask(1, action1));

        // after cool-down period passes start another task
        tracker.onTaskStart(action2, createTask(2, action2));

        // a new period should not start because first task is still running
        var activity = testSearchActions ? tracker.getSearchSampleActivity() : tracker.getIndexSampleActivity();
        assertEquals(activity, new Activity(sample, start1, Instant.EPOCH, Instant.EPOCH));
    }

    public void testMergeActivityFromPersistentNodeNoOpenTasks() {
        var threadContext = nonOperatorThreadContext();
        var hasSearchRole = randomBoolean();

        var tracker = TaskActivityTracker.build(
            Clock.systemUTC(),
            coolDown,
            hasSearchRole,
            threadContext,
            TEST_MAPPER,
            mock(TaskManager.class)
        );

        assertEquals(tracker.getSearchSampleActivity(), Activity.EMPTY);
        assertEquals(tracker.getIndexSampleActivity(), Activity.EMPTY);

        var thisSearchBefore = ActivityTests.randomActivity();
        var thisIndexBefore = ActivityTests.randomActivity();

        tracker.mergeActivity(thisSearchBefore, thisIndexBefore);

        assertEquals(tracker.getSearchSampleActivity(), thisSearchBefore);
        assertEquals(tracker.getIndexSampleActivity(), thisIndexBefore);

        var otherSearch = ActivityTests.randomActivity();
        var otherIndex = ActivityTests.randomActivity();

        tracker.mergeActivity(otherSearch, otherIndex);

        assertEquals(tracker.getSearchSampleActivity(), Activity.merge(Stream.of(otherSearch, thisSearchBefore), coolDown));
        assertEquals(tracker.getIndexSampleActivity(), Activity.merge(Stream.of(otherIndex, thisIndexBefore), coolDown));

    }

    public void testMergeActivityFromPersistentNodeOpenTasks() {
        var now = Instant.now();
        var taskStart = now.minus(ActivityTests.randomDuration(Duration.ZERO, Duration.ofHours(1)));
        var mergeTime = now.plus(ActivityTests.randomDuration(Duration.ZERO, Duration.ofHours(1)));

        var clock = createClock(
            taskStart, // onTaskStart search
            taskStart, // onTaskStart index
            Instant.EPOCH, // search sample, use epoch to get un-extended activity
            Instant.EPOCH, // index sample, use epoch to get un-extended activity
            mergeTime, // update time
            Instant.EPOCH, // search sample, use epoch to get un-extended activity
            Instant.EPOCH // index sample, use epoch to get un-extended activity
        );

        var tracker = TaskActivityTracker.build(
            clock,
            coolDown,
            randomBoolean(),
            nonOperatorThreadContext(),
            TEST_MAPPER,
            mock(TaskManager.class)
        );

        var searchAction = randomFrom(SEARCH_ACTION, BOTH_ACTION);
        tracker.onTaskStart(searchAction, createTask(123, searchAction));
        var indexAction = randomFrom(INDEX_ACTION, BOTH_ACTION);
        tracker.onTaskStart(indexAction, createTask(456, indexAction));

        // used Instant.EPOCH as the clock to extract un-extended timestamp
        var thisSearch = tracker.getSearchSampleActivity();
        var thisIndex = tracker.getIndexSampleActivity();

        // activity in recent past so can overlap with trackers activity
        var otherSearch = ActivityTests.randomActivity();
        var otherIndex = ActivityTests.randomActivity();

        tracker.mergeActivity(otherSearch, otherIndex);

        assertEquals(
            tracker.getSearchSampleActivity(),
            Activity.merge(Stream.of(otherSearch, thisSearch.extendCurrentPeriod(mergeTime)), coolDown)
        );
        assertEquals(
            tracker.getIndexSampleActivity(),
            Activity.merge(Stream.of(otherIndex, thisIndex.extendCurrentPeriod(mergeTime)), coolDown)
        );
    }

    static Activity pickActivityForAction(String actionTested, TaskActivityTracker tracker) {
        return switch (actionTested) {
            case SEARCH_ACTION -> tracker.getSearchSampleActivity();
            case INDEX_ACTION -> tracker.getIndexSampleActivity();
            default -> throw new IllegalArgumentException("only use with SEARCH and INDEX action");
        };
    }

    private SecurityContext createSecurityContext(User user) {
        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getUser()).thenReturn(user);
        return securityContext;
    }

    private static Task createTask(long taskId, String action) {
        var task = mock(Task.class);
        when(task.getId()).thenReturn(taskId);
        when(task.getAction()).thenReturn(action);
        return task;
    }

    private static Clock createClock(List<Instant> times) {
        return createClock(times.toArray(new Instant[0]));
    }

    private static Clock createClock(Instant... times) {
        var iter = Arrays.stream(times).iterator();
        return new Clock() {
            @Override
            public ZoneId getZone() {
                return null;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return null;
            }

            @Override
            public Instant instant() {
                assert iter.hasNext() : "Too few mocked clock instants";
                return iter.next();
            }
        };
    }

    private static ThreadContext operatorThreadContext() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY, AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR);
        return threadContext;
    }

    private static ThreadContext nonOperatorThreadContext() {
        return new ThreadContext(Settings.EMPTY);
    }

    private static void addTimeWithOffset(List<Instant> times, TemporalAmount offset) {
        var previous = times.get(times.size() - 1);
        times.add(previous.plus(offset));
    }
}
