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

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

class ReportGatherer {
    private static final Logger log = LogManager.getLogger(ReportGatherer.class);
    static final double MAX_JITTER_FACTOR = 0.25;

    private final MeteringService service;
    private final Consumer<List<UsageRecord>> reporter;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final Clock clock;
    private final TimeValue reportPeriod;
    private final Duration reportPeriodDuration;
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
        this(service, reporter, threadPool, executorName, reportPeriod, Clock.systemUTC());
    }

    ReportGatherer(
        MeteringService service,
        Consumer<List<UsageRecord>> reporter,
        ThreadPool threadPool,
        String executorName,
        TimeValue reportPeriod,
        Clock clock
    ) {
        this.service = service;
        this.reporter = reporter;
        this.threadPool = threadPool;
        this.executor = threadPool.executor(executorName);
        this.reportPeriod = reportPeriod;
        this.clock = clock;

        reportPeriodDuration = Duration.ofNanos(reportPeriod.nanos());
        // report period needs to fit evenly into 1 hour for calculateSampleTimestamp to work properly
        if (reportPeriodDuration.multipliedBy(Duration.ofHours(1).dividedBy(reportPeriodDuration)).equals(Duration.ofHours(1)) == false) {
            throw new IllegalArgumentException(Strings.format("Report period [%s] needs to fit evenly into 1 hour", reportPeriod));
        }
        this.sourceId = "es-" + service.nodeId();
    }

    void start() {
        Instant now = Instant.now(clock);
        // we want to be sure to produce samples for every single sampling period, starting with the current period
        Instant sampleTimestamp = calculateSampleTimestamp(now, reportPeriodDuration);
        long nanosToNextPeriod = now.until(sampleTimestamp.plus(reportPeriodDuration), ChronoUnit.NANOS);
        // schedule the first run towards the end of the current period so that collectors are more likely to have metrics available
        TimeValue timeToNextRun = TimeValue.timeValueNanos(nanosToNextPeriod * 9 / 10);
        nextRun = threadPool.schedule(() -> gatherReports(sampleTimestamp), timeToNextRun, executor);
        log.trace("Scheduled first task");
    }

    boolean cancel() {
        // don't need to synchronize anything, cancel is idempotent
        boolean cancelled = cancel == false;
        cancel = true;
        var run = nextRun;
        if (run != null) {
            run.cancel();  // try to optimistically stop the scheduled next run
            nextRun = null;
        }
        return cancelled;
    }

    private void gatherReports(Instant sampleTimestamp) {
        log.trace("starting to gather reports");
        if (cancel) return; // cancelled - nothing to do

        Instant startedAt = Instant.now(clock);
        Instant nextSampleTimestamp = sampleTimestamp.plus(reportPeriodDuration);
        try {
            reportMetrics(startedAt.truncatedTo(ChronoUnit.MILLIS), sampleTimestamp);
        } catch (Exception e) {
            log.error("Exception thrown reporting metrics", e);
            // then reschedule
        } finally {
            Instant completedAt = Instant.now(clock);
            checkRuntime(startedAt, completedAt);
            TimeValue timeToNextRun = timeToNextRun(completedAt, nextSampleTimestamp);
            if (cancel == false) {
                try {
                    // schedule the next run
                    nextRun = threadPool.schedule(() -> gatherReports(nextSampleTimestamp), timeToNextRun, executor);
                    log.trace(
                        () -> Strings.format("scheduled next run in %s.%s seconds", timeToNextRun.getSeconds(), timeToNextRun.getMillis())
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

    private void checkRuntime(Instant startedAt, Instant completedAt) {
        long runtime = startedAt.until(completedAt, ChronoUnit.MILLIS);
        if (runtime > reportPeriodDuration.toMillis()) {
            // TODO: report to somewhere that cares
            log.error(
                "Gathering metrics took {} [reportPeriod: {}], delaying the report schedule!",
                TimeValue.timeValueMillis(runtime),
                reportPeriod
            );
        } else if (runtime > reportPeriodDuration.toMillis() / 2) {
            // TODO: report to somewhere that cares
            log.warn("Gathering metrics took {}", TimeValue.timeValueMillis(runtime));
        }
    }

    private TimeValue timeToNextRun(Instant now, Instant nextSampleTimestamp) {
        // add up to 25% jitter
        double jitterFactor = Randomness.get().nextDouble() * MAX_JITTER_FACTOR;
        Duration jitter = Duration.ofNanos((long) (reportPeriodDuration.toNanos() * jitterFactor));
        long nanosUntilNextRun = now.until(nextSampleTimestamp.plus(jitter), ChronoUnit.NANOS);
        return nanosUntilNextRun > 0 ? TimeValue.timeValueNanos(nanosUntilNextRun) : TimeValue.ZERO;
    }

    private void reportMetrics(Instant now, Instant sampleTimestamp) {
        List<UsageRecord> records = service.getMetrics().map(v -> switch (v.measurementType()) {
            case COUNTER -> getRecordForCount(v.id(), v.type(), v.value(), v.metadata(), v.settings(), now);
            case SAMPLED -> getRecordForSample(v.id(), v.type(), v.value(), v.metadata(), v.settings(), sampleTimestamp);
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
        Instant sampleTimestamp
    ) {
        return new UsageRecord(
            generateId(metric, sampleTimestamp),
            sampleTimestamp,
            new UsageMetrics(type, null, value, reportPeriod, null, settings, null),
            new UsageSource(sourceId, service.projectId(), metadata)
        );
    }
}
