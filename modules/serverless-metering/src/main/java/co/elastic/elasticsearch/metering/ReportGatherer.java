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

import co.elastic.elasticsearch.metering.reports.UsageMetrics;
import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.metering.reports.UsageSource;

import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

class ReportGatherer {
    private static final Logger log = LogManager.getLogger(ReportGatherer.class);

    private final MeteringService service;
    private final Consumer<List<UsageRecord>> reporter;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final TimeValue reportPeriod;
    private final Duration reportPeriodDuration;
    private final StopWatch runTimer = new StopWatch();
    private final String sourceId;

    private volatile boolean cancel;
    private volatile Scheduler.Cancellable nextRun;

    ReportGatherer(
        MeteringService service,
        Consumer<List<UsageRecord>> reporter,
        ThreadPool threadPool,
        String executorName,
        TimeValue reportPeriod
    ) {
        this.service = service;
        this.reporter = reporter;
        this.threadPool = threadPool;
        this.executor = threadPool.executor(executorName);
        this.reportPeriod = reportPeriod;

        reportPeriodDuration = Duration.ofNanos(reportPeriod.nanos());
        // report period needs to fit evenly into 1 hour for calculateSampleTimestamp to work properly
        if (reportPeriodDuration.multipliedBy(Duration.ofHours(1).dividedBy(reportPeriodDuration)).equals(Duration.ofHours(1)) == false) {
            throw new IllegalArgumentException(Strings.format("Report period [%s] needs to fit evenly into 1 hour", reportPeriod));
        }
        this.sourceId = "es-" + service.nodeId();
    }

    void start() {
        nextRun = threadPool.schedule(this::gatherReports, reportPeriod, executor);
        log.trace("Scheduled first task");
    }

    boolean cancel() {
        // don't need to synchronize anything, cancel is idempotent
        boolean cancelled = cancel == false;
        cancel = true;
        var run = nextRun;
        if (run != null) run.cancel();  // try to optimistically stop the scheduled next run
        return cancelled;
    }

    private void gatherReports() {
        log.trace("starting to gather reports");
        if (cancel) return; // cancelled - nothing to do

        runTimer.start();
        try {
            reportMetrics();
        } catch (Exception e) {
            log.error("Exception thrown reporting metrics", e);
            // then reschedule
        } finally {
            runTimer.stop();

            // work out how long it took
            TimeValue remainingNanos = getRemainingNanos();

            if (cancel == false) {
                try {
                    // schedule the next run
                    // TODO: jitter within the expected schedules
                    nextRun = threadPool.schedule(this::gatherReports, remainingNanos, executor);
                    log.trace(
                        () -> Strings.format("scheduled next run in %s.%s seconds", remainingNanos.getSeconds(), remainingNanos.getMillis())
                    );

                } catch (EsRejectedExecutionException e) {
                    nextRun = null;
                    if (e.isExecutorShutdown()) {
                        // ok - thread pool shutting down
                        log.trace("Not rescheduling report gathering because this node is being shutdown", e);
                    } else {
                        log.error("Unexpected exception whilst re-scheduling report gathering", e);
                        assert false : e;
                        // exception can't go anywhere, just stop here
                    }
                }
            }

        }
    }

    private TimeValue getRemainingNanos() {
        TimeValue runtime = runTimer.lastTaskTime();
        long remainingNanos = reportPeriod.nanos() - runtime.nanos();
        if (remainingNanos < 0) {
            // TODO: report to somewhere that cares
            log.error("Gathering metrics took longer than the report period ({})!", runtime);
            remainingNanos = 0;
        } else if (remainingNanos < reportPeriod.nanos() / 2) {
            // TODO: report to somewhere that cares
            log.warn("Gathering metrics took {}", runtime);
        }
        return TimeValue.timeValueNanos(remainingNanos);
    }

    private void reportMetrics() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        List<UsageRecord> records = service.getMetrics().map(v -> switch (v.measurementType()) {
            case COUNTER -> getRecordForCount(v.id(), v.type(), v.value(), v.metadata(), v.settings(), now);
            case SAMPLED -> getRecordForSample(v.id(), v.type(), v.value(), v.metadata(), v.settings(), now);
        }).toList();

        reporter.accept(records);
    }

    private static String generateId(String key, Instant time) {
        return key + "-" + time.truncatedTo(ChronoUnit.SECONDS);
    }

    static Instant calculateSampleTimestamp(Instant now, Duration reportPeriod) {
        // this essentially calculates 'now' mod the reportPeriod, relative to hour timeslots
        // this gets us a consistent rounded report period, regardless of where in that period
        // the record is actually being calculated
        assert reportPeriod.compareTo(Duration.ofHours(1)) <= 0;
        assert reportPeriod.compareTo(Duration.ofSeconds(1)) >= 0;

        // round to the hour as a baseline
        Instant hour = now.truncatedTo(ChronoUnit.HOURS);
        // get the time into the hour we are
        Duration intoHour = Duration.between(hour, now);
        // get how many times reportPeriod divides into the duration
        long times = intoHour.dividedBy(reportPeriod);
        // get our floor'd timestamp
        return hour.plus(reportPeriod.multipliedBy(times));
    }

    private UsageRecord getRecordForCount(
        String metric,
        String type,
        long count,
        Map<String, String> metadata,
        Map<String, Object> settings,
        Instant now
    ) {
        return new UsageRecord(
            generateId(metric, now),
            now,
            new UsageMetrics(type, null, count, reportPeriod, null, settings, null),
            new UsageSource(sourceId, service.projectId(), metadata)
        );
    }

    private UsageRecord getRecordForSample(
        String metric,
        String type,
        long value,
        Map<String, String> metadata,
        Map<String, Object> settings,
        Instant now
    ) {
        Instant timestamp = calculateSampleTimestamp(now, reportPeriodDuration);

        return new UsageRecord(
            generateId(metric, timestamp),
            timestamp,
            new UsageMetrics(type, null, value, reportPeriod, null, settings, null),
            new UsageSource(sourceId, service.projectId(), metadata)
        );
    }
}
