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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TaskActivityTracker {
    private static final Logger log = LogManager.getLogger(TaskActivityTracker.class);
    public static final Setting<TimeValue> COOL_DOWN_PERIOD = Setting.timeSetting(
        "metering.activity_tracker.cool_down_period",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(120),
        Setting.Property.NodeScope
    );

    /**
     * A subset of the InternalUsers which will not be tracked. Other instances of
     * InternalUser will be tracked (if isOperator==false and actionTier!=NEITHER).
     *
     * All tracked InternalUser instances are listed below:
     *  InternalUsers.ASYNC_SEARCH_USER,
     *  InternalUsers.STORAGE_USER,
     *  InternalUsers.DATA_STREAM_LIFECYCLE_USER,
     *  InternalUsers.SYNONYMS_USER,
     *  InternalUsers.LAZY_ROLLOVER_USER
     */
    static final Set<InternalUser> INTERNAL_USERS_TO_IGNORE = Set.of(
        InternalUsers.SYSTEM_USER,
        InternalUsers.XPACK_USER,
        InternalUsers.XPACK_SECURITY_USER,
        InternalUsers.SECURITY_PROFILE_USER
    );

    private final boolean hasSearchRole;
    private final ThreadContext threadContext;
    private final Clock clock;
    private final Duration coolDownPeriod;
    private final ActionTier.Mapper actionTierMapper;
    private final SecurityContext securityContext;

    private volatile Activity search = Activity.EMPTY;
    private volatile Activity index = Activity.EMPTY;
    private final Set<Long> searchTaskIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> indexTaskIds = ConcurrentHashMap.newKeySet();
    private final Set<Long> bothTaskIds = ConcurrentHashMap.newKeySet();

    private TaskActivityTracker(
        Clock clock,
        TimeValue coolDownPeriod,
        boolean hasSearchRole,
        ThreadContext threadContext,
        ActionTier.Mapper actionTierMapper,
        SecurityContext securityContext
    ) {
        // To simplify testing, clock.instant() should be called at most once in every public method
        this.clock = clock;
        this.coolDownPeriod = Duration.ofMillis(coolDownPeriod.millis());
        this.hasSearchRole = hasSearchRole;
        this.threadContext = threadContext;
        this.actionTierMapper = actionTierMapper;
        this.securityContext = securityContext;
    }

    public static TaskActivityTracker build(
        Clock clock,
        TimeValue coolDownPeriod,
        boolean hasSearchRole,
        ThreadContext threadContext,
        ActionTier.Mapper actionTierMapper,
        TaskManager taskManager
    ) {
        return build(
            clock,
            coolDownPeriod,
            hasSearchRole,
            threadContext,
            actionTierMapper,
            taskManager,
            new SecurityContext(Settings.EMPTY, threadContext)
        );
    }

    static TaskActivityTracker build(
        Clock clock,
        TimeValue coolDownPeriod,
        boolean hasSearchRole,
        ThreadContext threadContext,
        ActionTier.Mapper actionTierMapper,
        TaskManager taskManager,
        SecurityContext securityContext
    ) {
        var tracker = new TaskActivityTracker(clock, coolDownPeriod, hasSearchRole, threadContext, actionTierMapper, securityContext);
        taskManager.registerRemovedTaskListener(tracker::onTaskFinish);
        return tracker;
    }

    public Activity getIndexSampleActivity() {
        return noIndexTaskIsRunning() ? index : index.extendCurrentPeriod(clock.instant());
    }

    public Activity getSearchSampleActivity() {
        return noSearchTaskIsRunning() ? search : search.extendCurrentPeriod(clock.instant());
    }

    void onTaskStart(String action, Task task) {
        if (log.isDebugEnabled()) {
            log.debug(
                "Tracking: {},{},{},{}",
                isOperator() ? "operator" : "not_operator",
                getInternalUserName(),
                actionTierMapper.toTier(action),
                action
            );
        }

        if (isOperator()) {
            log.debug("Skip because user has operator privilege, action: " + action);
            return;
        } else if (isUntrackedInternalUser()) {
            log.debug("Skip because is internal user, action: " + action);
            return;
        }

        var actionTier = actionTierMapper.toTier(action);
        var now = clock.instant();

        Activity searchCurrent = search;
        Activity indexCurrent = index;
        switch (actionTier) {
            case SEARCH -> {
                if (hasSearchRole == false) {
                    log.debug("found action with SEARCH tier but node not search role: " + action);
                }

                if (noSearchTaskIsRunning() && coolDownPeriodHasElapsed(searchCurrent, now)) {
                    search = searchCurrent.makeNewPeriod(now);
                }
                searchTaskIds.add(task.getId());
            }
            case INDEX -> {
                if (hasSearchRole) {
                    log.debug("found action with INDEX tier but node not index role: " + action);
                }

                if (noIndexTaskIsRunning() && coolDownPeriodHasElapsed(indexCurrent, now)) {
                    index = indexCurrent.makeNewPeriod(now);
                }
                indexTaskIds.add(task.getId());
            }
            case BOTH -> {
                if (noSearchTaskIsRunning() && coolDownPeriodHasElapsed(searchCurrent, now)) {
                    search = searchCurrent.makeNewPeriod(now);
                }
                if (noIndexTaskIsRunning() && coolDownPeriodHasElapsed(indexCurrent, now)) {
                    index = indexCurrent.makeNewPeriod(now);
                }
                bothTaskIds.add(task.getId());
            }
        }
    }

    void onTaskFinish(Task task) {
        var actionTier = actionTierMapper.toTier(task.getAction());
        var now = clock.instant();

        if (actionTier == ActionTier.SEARCH) {
            if (searchTaskIds.remove(task.getId())) {
                search = search.extendCurrentPeriod(now);
            }
        }
        if (actionTier == ActionTier.INDEX) {
            if (indexTaskIds.remove(task.getId())) {
                index = index.extendCurrentPeriod(now);
            }
        }
        if (actionTier == ActionTier.BOTH) {
            if (bothTaskIds.remove(task.getId())) {
                search = search.extendCurrentPeriod(now);
                index = index.extendCurrentPeriod(now);
            }
        }
    }

    private boolean coolDownPeriodHasElapsed(Activity activity, Instant now) {
        return activity.lastActivityRecentPeriod().isBefore(now.minus(coolDownPeriod));
    }

    private boolean noSearchTaskIsRunning() {
        return searchTaskIds.isEmpty() && bothTaskIds.isEmpty();
    }

    private boolean noIndexTaskIsRunning() {
        return indexTaskIds.isEmpty() && bothTaskIds.isEmpty();
    }

    private boolean isUntrackedInternalUser() {
        if (securityContext.getUser() instanceof InternalUser iu) {
            return INTERNAL_USERS_TO_IGNORE.contains(iu);
        }
        return false;
    }

    private String getInternalUserName() {
        return securityContext.getUser() instanceof InternalUser iu ? iu.principal() : null;
    }

    private String getUserPrivilege() {
        return threadContext.getHeader(AuthenticationField.PRIVILEGE_CATEGORY_KEY);
    }

    private boolean isOperator() {
        return AuthenticationField.PRIVILEGE_CATEGORY_VALUE_OPERATOR.equals(getUserPrivilege());
    }
}
