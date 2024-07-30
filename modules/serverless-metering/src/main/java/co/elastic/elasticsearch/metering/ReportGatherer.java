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

import co.elastic.elasticsearch.metering.reports.MeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.reports.UsageMetrics;
import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.metering.reports.UsageSource;
import co.elastic.elasticsearch.metrics.CounterMetricsCollector;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.SampleTimestampUtils.calculateSampleTimestamp;
import static co.elastic.elasticsearch.metering.SampleTimestampUtils.interpolateValueForTimestamp;

class ReportGatherer {
    private static final Logger log = LogManager.getLogger(ReportGatherer.class);
    static final double MAX_JITTER_FACTOR = 0.25;
    private static final Duration MAX_BACKFILL_LOOKBACK = Duration.ofHours(24);

    private final List<CounterMetricsCollector> counterMetricsCollectors;
    private final List<SampledMetricsCollector> sampledMetricsCollectors;
    private final SampledMetricsTimeCursor sampledMetricsTimeCursor;
    private final MeteringUsageRecordPublisher usageRecordPublisher;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final Clock clock;
    private final TimeValue reportPeriod;
    private final Duration reportPeriodDuration;
    final int maxPeriodsLookback;
    private final String sourceId;
    private final String projectId;

    private volatile boolean cancel;
    private volatile Scheduler.Cancellable nextRun;
    private volatile Map<String, MetricValue> lastSampledMetricValues;

    ReportGatherer(
        String nodeId,
        String projectId,
        List<CounterMetricsCollector> counterMetricsCollectors,
        List<SampledMetricsCollector> sampledMetricsCollectors,
        SampledMetricsTimeCursor sampledMetricsTimeCursor,
        MeteringUsageRecordPublisher usageRecordPublisher,
        ThreadPool threadPool,
        ExecutorService executor,
        TimeValue reportPeriod,
        Clock clock
    ) {
        this.projectId = projectId;
        this.counterMetricsCollectors = counterMetricsCollectors;
        this.sampledMetricsCollectors = sampledMetricsCollectors;
        this.sampledMetricsTimeCursor = sampledMetricsTimeCursor;
        this.usageRecordPublisher = usageRecordPublisher;
        this.threadPool = threadPool;
        this.executor = executor;
        this.reportPeriod = reportPeriod;
        this.clock = clock;

        reportPeriodDuration = Duration.ofNanos(reportPeriod.nanos());
        // report period needs to fit evenly into 1 hour for calculateSampleTimestamp to work properly
        if (reportPeriodDuration.multipliedBy(Duration.ofHours(1).dividedBy(reportPeriodDuration)).equals(Duration.ofHours(1)) == false) {
            throw new IllegalArgumentException(Strings.format("Report period [%s] needs to fit evenly into 1 hour", reportPeriod));
        }
        this.maxPeriodsLookback = (int) MAX_BACKFILL_LOOKBACK.dividedBy(reportPeriodDuration);
        this.sourceId = "es-" + nodeId;
    }

    void start() {
        Instant now = Instant.now(clock);
        // we want to be sure to produce samples for every single sampling period, starting with the current period
        Instant sampleTimestamp = calculateSampleTimestamp(now, reportPeriodDuration);
        long nanosToNextPeriod = now.until(sampleTimestamp.plus(reportPeriodDuration), ChronoUnit.NANOS);
        // schedule the first run towards the end of the current period so that collectors are more likely to have metrics available
        TimeValue timeToNextRun = TimeValue.timeValueNanos(nanosToNextPeriod * 9 / 10);
        nextRun = threadPool.schedule(() -> gatherAndSendReports(sampleTimestamp), timeToNextRun, executor);
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

    private void gatherAndSendReports(Instant sampleTimestamp) {
        log.trace("starting to gather reports");
        if (cancel) {
            return; // cancelled - nothing to do
        }

        Instant startedAt = Instant.now(clock);

        var reportsSent = collectMetricsAndSendReport(startedAt.truncatedTo(ChronoUnit.MILLIS), sampleTimestamp);

        Instant completedAt = Instant.now(clock);
        checkRuntime(startedAt, completedAt);

        if (cancel == false) {
            try {
                final Instant nextSampleTimestamp = sampleTimestamp.plus(reportPeriodDuration);
                final Instant nextSampleTimestampToGather = (reportsSent || completedAt.isAfter(nextSampleTimestamp))
                    ? nextSampleTimestamp
                    : sampleTimestamp;

                var timeToNextRun = timeToNextRun(reportsSent, completedAt, nextSampleTimestampToGather, reportPeriodDuration);
                // schedule the next run
                nextRun = threadPool.schedule(() -> gatherAndSendReports(nextSampleTimestampToGather), timeToNextRun, executor);
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

    static TimeValue timeToNextRun(boolean reportsSent, Instant now, Instant nextSampleTimestamp, Duration reportPeriodDuration) {
        final long nanosUntilNextRun;
        if (reportsSent) {
            // add up to 25% jitter
            double jitterFactor = Randomness.get().nextDouble() * 0.25;
            Duration jitter = Duration.ofNanos((long) (reportPeriodDuration.toNanos() * jitterFactor));
            nanosUntilNextRun = now.until(nextSampleTimestamp.plus(jitter), ChronoUnit.NANOS);
        } else {
            // We want to retry "faster", but avoid doing that immediately, even if we are late for the next sample timestamp
            // add or remove up to 25% jitter
            double jitterFactor = (Randomness.get().nextDouble() * 0.5) - 0.25;
            final long reducedPeriodNanos = reportPeriodDuration.toNanos() / 10;
            final long jitterNanos = (long) (reducedPeriodNanos * jitterFactor);
            nanosUntilNextRun = reducedPeriodNanos + jitterNanos;
        }
        return nanosUntilNextRun > 0 ? TimeValue.timeValueNanos(nanosUntilNextRun) : TimeValue.ZERO;
    }

    private boolean sendReport(List<UsageRecord> report) {
        try {
            usageRecordPublisher.sendRecords(report);
            return true;
        } catch (Exception e) {
            log.warn("Exception thrown reporting metrics", e);
        }
        return false;
    }

    private boolean collectMetricsAndSendReport(Instant now, Instant sampleTimestamp) {
        List<UsageRecord> records = new ArrayList<>();

        List<CounterMetricsCollector.MetricValues> counterMetricValuesList = counterMetricsCollectors.stream()
            .map(CounterMetricsCollector::getMetrics)
            .toList();

        counterMetricValuesList.forEach(counterMetricValues -> {
            for (var v : counterMetricValues) {
                records.add(getRecordForCount(v.id(), v.type(), v.value(), v.metadata(), v.settings(), now));
            }
        });

        var timestamps = sampledMetricsTimeCursor.generateSampleTimestamps(sampleTimestamp, reportPeriod);
        var timestampsToSend = Iterators.toList(Iterators.limit(timestamps, maxPeriodsLookback));

        if (timestampsToSend.size() > 1) {
            var backfillDuration = Duration.between(timestamps.last(), sampleTimestamp);
            var periodsToSend = backfillDuration.dividedBy(reportPeriodDuration);
            assert periodsToSend > 0;
            if (lastSampledMetricValues == null) {
                log.warn(
                    "Skip backfilling [{}-{}] -- missing the necessary state to calculate backfill samples. We will drop [{}] sample(s).",
                    timestampsToSend.get(timestampsToSend.size() - 1),
                    timestampsToSend.get(0),
                    periodsToSend
                );
            } else {
                if (periodsToSend > maxPeriodsLookback) {
                    log.warn(
                        "Partially backfilling [{}-{}] -- the backfill window [{}-{}] is grater than the maximum lookback [{}]. "
                            + "We will drop [{}] sample(s).",
                        timestampsToSend.get(timestampsToSend.size() - 1),
                        timestampsToSend.get(0),
                        timestamps.last(),
                        sampleTimestamp,
                        MAX_BACKFILL_LOOKBACK,
                        periodsToSend - maxPeriodsLookback
                    );
                } else {
                    log.info(
                        "Backfilling [{}-{}] with [{}] sample(s)",
                        timestampsToSend.get(timestampsToSend.size() - 1),
                        timestampsToSend.get(0),
                        periodsToSend
                    );
                }
            }
        }

        boolean canAdvanceSampledMetricsTimeCursor = false;
        List<MetricValue> currentSampledMetricValues = new ArrayList<>();
        if (timestampsToSend.isEmpty() == false) {
            for (SampledMetricsCollector sampledMetricsCollector : sampledMetricsCollectors) {
                try {
                    var sampledMetricValues = sampledMetricsCollector.getMetrics();
                    if (sampledMetricValues.isEmpty()) {
                        log.info("[{}] is not ready for collect yet", sampledMetricsCollector.getClass().getName());
                        break;
                    } else {
                        for (var v : sampledMetricValues.get()) {
                            currentSampledMetricValues.add(v);
                            // we will only receive samples on the node hosting the MeteringIndexInfoTask
                            // if MetricValues is empty (or if there's no timestamps to send) we must not advance the committed timestamp
                            canAdvanceSampledMetricsTimeCursor = true;
                        }
                    }
                } catch (Exception e) {
                    log.error(
                        Strings.format("Exception thrown collecting sampled metrics from %s", sampledMetricsCollector.getClass().getName()),
                        e
                    );
                    canAdvanceSampledMetricsTimeCursor = false;
                }
            }

            if (timestampsToSend.size() <= 1 || lastSampledMetricValues == null) {
                buildRecordsWithoutBackfill(sampleTimestamp, currentSampledMetricValues, records);
            } else {
                buildRecordsWithBackfill(sampleTimestamp, timestampsToSend, currentSampledMetricValues, timestamps.last(), records);
            }
        }

        if (records.isEmpty()) {
            log.info("No usage record generated during this metrics collection");
            return true;
        }
        if (sendReport(records)) {
            for (var metricValues : counterMetricValuesList) {
                metricValues.commit();
            }
            if (canAdvanceSampledMetricsTimeCursor) {
                var committed = sampledMetricsTimeCursor.commitUpTo(sampleTimestamp);
                if (committed) {
                    lastSampledMetricValues = currentSampledMetricValues.stream()
                        .collect(Collectors.toUnmodifiableMap(MetricValue::id, Function.identity()));
                }
                log.info("Updating committed timestamp to [{}], success: [{}]", sampleTimestamp, committed);
                return committed;
            }
            return true;
        }
        return false;
    }

    private void buildRecordsWithoutBackfill(
        Instant sampleTimestamp,
        List<MetricValue> currentSampledMetricValues,
        List<UsageRecord> records
    ) {
        for (var v : currentSampledMetricValues) {
            records.add(getRecordForSample(v.id(), v.type(), v.value(), v.metadata(), v.settings(), sampleTimestamp));
        }
    }

    private void buildRecordsWithBackfill(
        Instant sampleTimestamp,
        List<Instant> timestampsToSend,
        List<MetricValue> currentSampledMetricValues,
        Instant latestCommittedTimestamp,
        List<UsageRecord> records
    ) {
        for (var v : currentSampledMetricValues) {
            var previousMetricValue = lastSampledMetricValues.get(v.id());
            if (previousMetricValue == null) {
                log.info("Metric [{}] of type [{}] does not have a previous value, not backfilling", v.id(), v.type());
                records.add(getRecordForSample(v.id(), v.type(), v.value(), v.metadata(), v.settings(), sampleTimestamp));
            } else {
                for (var timestamp : timestampsToSend) {
                    var previousValue = previousMetricValue.value();
                    long value = interpolateValueForTimestamp(
                        sampleTimestamp,
                        v.value(),
                        latestCommittedTimestamp,
                        previousValue,
                        timestamp
                    );
                    log.info(
                        "Backfilling metric [{}] of type [{}] with value [{}] (prev: [{}], current [{}]) "
                            + "for time [{}] (prev: [{}], current: [{}])",
                        v.id(),
                        v.type(),
                        value,
                        previousValue,
                        v.value(),
                        timestamp,
                        latestCommittedTimestamp,
                        sampleTimestamp
                    );
                    records.add(getRecordForSample(v.id(), v.type(), value, v.metadata(), v.settings(), timestamp));
                }
            }
        }
    }

    private static String generateId(String key, Instant time) {
        return key + "-" + time.truncatedTo(ChronoUnit.SECONDS);
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
            new UsageSource(sourceId, projectId, metadata)
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
            new UsageSource(sourceId, projectId, metadata)
        );
    }
}
