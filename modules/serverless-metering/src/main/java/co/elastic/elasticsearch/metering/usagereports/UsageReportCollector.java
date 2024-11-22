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

import co.elastic.elasticsearch.metering.usagereports.SampledMetricsTimeCursor.Timestamps;
import co.elastic.elasticsearch.metering.usagereports.publisher.MeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageMetrics;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageSource;
import co.elastic.elasticsearch.metrics.CounterMetricsProvider;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.metering.usagereports.SampleTimestampUtils.calculateSampleTimestamp;

/**
 * Periodically collects counter and sampled metrics from metrics providers to produce a usage report.
 *
 * The usage report, a collection of {@link UsageRecord}s, is then published using {@link MeteringUsageRecordPublisher}.
 * {@link UsageReportCollector} keeps track of the last sample timestamp in cluster state and backfills usage records in case
 * it detects missing samples.
 */
class UsageReportCollector {
    private static final Logger log = LogManager.getLogger(UsageReportCollector.class);
    static final double MAX_JITTER_FACTOR = 0.25;
    static final Duration MAX_BACKFILL_LOOKBACK = Duration.ofHours(24);
    static final int MAX_CONSTANT_BACKFILL = 2;

    static final String METERING_REPORTS_TOTAL = "es.metering.reporting.runs.total";
    static final String METERING_REPORTS_FAILED_TOTAL = "es.metering.reporting.runs.failed.total";
    static final String METERING_REPORTS_DELAYED_TOTAL = "es.metering.reporting.runs.delayed.total";
    static final String METERING_REPORTS_RETRIED_TOTAL = "es.metering.reporting.runs.retried.total";
    static final String METERING_REPORTS_TIME = "es.metering.reporting.runs.time";
    static final String METERING_REPORTS_SENT_TOTAL = "es.metering.reporting.sent.total";
    static final String METERING_REPORTS_BACKFILL_TOTAL = "es.metering.reporting.backfill.total";
    static final String METERING_REPORTS_BACKFILL_PERIODS_TOTAL = "es.metering.reporting.backfill.periods.total";
    static final String METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL = "es.metering.reporting.backfill.dropped.total";
    static final String METERING_REPORTS_PERIOD = "es_metering_reporting_period";

    private final List<CounterMetricsProvider> counterMetricsProviders;
    private final List<SampledMetricsProvider> sampledMetricsProviders;
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

    private final LongCounter reportsTotalCounter;
    private final LongCounter reportsFailedCounter;
    private final LongCounter reportsDelayedCounter;
    private final LongCounter reportsRetryCounter;
    private final LongHistogram reportsRunTime;
    private final LongCounter reportsSentTotalCounter;
    private final LongCounter reportsBackfillTotalCounter;
    private final LongCounter reportsBackfillPeriodsCounter;
    private final LongCounter reportsBackfillDroppedPeriodsCounter;

    private final AtomicReference<Scheduler.Cancellable> nextRun = new AtomicReference<>();
    private volatile Map<String, MetricValue> lastSampledMetricValues;

    UsageReportCollector(
        String nodeId,
        String projectId,
        List<CounterMetricsProvider> counterMetricsProviders,
        List<SampledMetricsProvider> sampledMetricsProviders,
        SampledMetricsTimeCursor sampledMetricsTimeCursor,
        MeteringUsageRecordPublisher usageRecordPublisher,
        ThreadPool threadPool,
        ExecutorService executor,
        TimeValue reportPeriod,
        Clock clock,
        MeterRegistry meterRegistry
    ) {
        this.projectId = projectId;
        this.counterMetricsProviders = counterMetricsProviders;
        this.sampledMetricsProviders = sampledMetricsProviders;
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

        this.reportsTotalCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_TOTAL,
            "The total number the reporting task run",
            "unit"
        );
        this.reportsFailedCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_FAILED_TOTAL,
            "The total number the reporting task failed",
            "unit"
        );
        this.reportsDelayedCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_DELAYED_TOTAL,
            "The total number the reporting schedule got delayed because a task run for longer than the reporting period",
            "unit"
        );
        this.reportsRetryCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_RETRIED_TOTAL,
            "The number of times the reporting task was re-scheduled in the same reporting period after failing to published a report",
            "unit"
        );
        this.reportsRunTime = meterRegistry.registerLongHistogram(METERING_REPORTS_TIME, "The run time of the reporting task, in ms", "ms");
        this.reportsSentTotalCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_SENT_TOTAL,
            "The number of times the reporting task successfully published a report",
            "unit"
        );
        this.reportsBackfillTotalCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_BACKFILL_TOTAL,
            "The number of times the reporting task had to backfill",
            "unit"
        );
        this.reportsBackfillPeriodsCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_BACKFILL_PERIODS_TOTAL,
            "The number of timeframes that need to be backfilled (number of timeframes we are behind)",
            "unit"
        );
        this.reportsBackfillDroppedPeriodsCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_BACKFILL_DROPPED_PERIODS_TOTAL,
            "The number of timeframes the reporting task had to drop during a backfill",
            "unit"
        );
    }

    void start() {
        Instant now = Instant.now(clock);
        // we want to be sure to produce samples for every single sampling period, starting with the current period
        Instant sampleTimestamp = calculateSampleTimestamp(now, reportPeriodDuration);
        long nanosToNextPeriod = now.until(sampleTimestamp.plus(reportPeriodDuration), ChronoUnit.NANOS);
        // schedule the first run towards the end of the current period so that collectors are more likely to have metrics available
        TimeValue timeToNextRun = TimeValue.timeValueNanos(nanosToNextPeriod * 9 / 10);
        nextRun.set(threadPool.schedule(() -> gatherAndSendReports(sampleTimestamp), timeToNextRun, executor));
        log.trace("Scheduled first task");
    }

    /**
     * Stops the collector and schedules a final collection of counter metrics only.
     */
    void stop() {
        var run = nextRun.getAndSet(null);
        if (run != null) {
            run.cancel();
            // run a final report (counter metrics only if stopped)
            try {
                executor.execute(() -> {
                    Instant startedAt = Instant.now(clock);
                    log.info("Running final usage report before stopping.");
                    gatherAndSendReports(calculateSampleTimestamp(startedAt, reportPeriodDuration));
                });
            } catch (RejectedExecutionException e) {
                log.error("Failed to schedule final report of counter metrics", e);
            }
        }
    }

    private boolean isStopped() {
        return nextRun.get() == null;
    }

    private Map<String, Object> metricAttributes() {
        if (isStopped() == false) {
            return Collections.emptyMap();
        }
        return Map.of("reporting_stopped", true);
    }

    private void gatherAndSendReports(Instant sampleTimestamp) {
        log.trace("starting to gather reports");

        reportsTotalCounter.incrementBy(1, metricAttributes());

        Instant startedAt = Instant.now(clock);
        var reportsSent = false;
        try {
            reportsSent = collectMetricsAndSendReport(startedAt.truncatedTo(ChronoUnit.MILLIS), sampleTimestamp);
        } catch (RuntimeException e) {
            reportsFailedCounter.incrementBy(1, metricAttributes());
            log.error("Unexpected exception whilst collecting metrics and sending reports [stopped: {}]", isStopped(), e);
        }

        Instant completedAt = Instant.now(clock);
        checkRuntime(startedAt, completedAt);

        Scheduler.Cancellable currentRun = nextRun.get();
        if (currentRun != null) {
            try {
                final Instant nextSampleTimestamp = sampleTimestamp.plus(reportPeriodDuration);
                var timeToNextRun = timeToNextRun(reportsSent, completedAt, nextSampleTimestamp, reportPeriodDuration);

                // next run is a retry if we do not proceed to the next sample period
                var isRetry = completedAt.plusMillis(timeToNextRun.getMillis()).isBefore(nextSampleTimestamp);
                final Instant nextSampleTimestampToGather;
                if (isRetry) {
                    nextSampleTimestampToGather = sampleTimestamp;
                    reportsRetryCounter.incrementBy(1, metricAttributes());
                } else {
                    nextSampleTimestampToGather = nextSampleTimestamp;
                }

                // schedule the next run
                var scheduledRun = threadPool.schedule(() -> gatherAndSendReports(nextSampleTimestampToGather), timeToNextRun, executor);
                if (nextRun.compareAndSet(currentRun, scheduledRun)) {
                    log.trace("scheduled next run in {}", timeToNextRun);
                } else {
                    scheduledRun.cancel(); // already stopped, cancel immediately
                }
            } catch (EsRejectedExecutionException e) {
                nextRun.set(null);
                if (e.isExecutorShutdown()) {
                    // ok - thread pool shutting down
                    log.trace("Not rescheduling report gathering because this node is being shutdown", e);
                } else {
                    log.error("Unexpected exception whilst re-scheduling report gathering", e);
                    assert false : e;
                    // exception can't go anywhere, just stop here
                }
            }
        } else if (reportsSent == false) {
            log.warn("Could not send final usage report before shutting down, some usage data might be lost.");
        }
    }

    private void checkRuntime(Instant startedAt, Instant completedAt) {
        long runtime = startedAt.until(completedAt, ChronoUnit.MILLIS);
        if (runtime > reportPeriodDuration.toMillis()) {
            reportsDelayedCounter.incrementBy(1, metricAttributes());
            log.error(
                "Gathering metrics took {} [reportPeriod: {}], delaying the report schedule!",
                TimeValue.timeValueMillis(runtime),
                reportPeriod
            );
        } else if (runtime > reportPeriodDuration.toMillis() / 2) {
            log.warn("Gathering metrics took {}", TimeValue.timeValueMillis(runtime));
        }
        reportsRunTime.record(runtime, Map.of(METERING_REPORTS_PERIOD, reportPeriod.millis()));
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
            if (usageRecordPublisher.sendRecords(report)) {
                reportsSentTotalCounter.incrementBy(1, metricAttributes());
                return true;
            }
        } catch (RuntimeException e) {
            log.warn("Exception when publishing usage records", e);
        }
        reportsFailedCounter.incrementBy(1, metricAttributes());
        return false;
    }

    private boolean collectMetricsAndSendReport(Instant now, Instant sampleTimestamp) {
        List<UsageRecord> records = new ArrayList<>();

        List<SampledMetricsProvider.MetricValues> sampledMetricValuesList = Collections.emptyList();
        List<CounterMetricsProvider.MetricValues> counterMetricValuesList = Collections.emptyList();
        Map<String, MetricValue> sampledMetricValuesById = Collections.emptyMap();

        // counter metrics have to be processed even if stopped to at least attempt publishing remaining in-memory counts a final time
        if (counterMetricsProviders.isEmpty() == false) {
            counterMetricValuesList = new ArrayList<>(counterMetricsProviders.size());
            for (var counterMetricsProvider : counterMetricsProviders) {
                try {
                    CounterMetricsProvider.MetricValues metricValues = counterMetricsProvider.getMetrics();
                    counterMetricValuesList.add(metricValues);
                    for (var value : metricValues) {
                        records.add(createUsageRecord(value, now));
                    }
                } catch (RuntimeException e) {
                    log.error(Strings.format("Failed to get counter metrics from %s", counterMetricsProvider.getClass().getName()), e);
                }
            }
            if (records.isEmpty()) {
                log.info("No counter usage records generated during this metrics collection [{}]", now);
            }
        }

        // sampled metrics can be skipped if stopped, we'll resume from the timestamp cursor on the next persistent task node
        if (isStopped() == false) {
            // only process sampled metrics if the SampledMetricsTimeCursor is ready (none empty timestamps)
            var timestamps = sampledMetricsTimeCursor.generateSampleTimestamps(sampleTimestamp, reportPeriod);
            if (timestamps.size() > 0 && sampledMetricsProviders.isEmpty() == false) {
                sampledMetricValuesList = new ArrayList<>(sampledMetricsProviders.size());
                for (SampledMetricsProvider sampledMetricsProvider : sampledMetricsProviders) {
                    try {
                        var sampledMetricValues = sampledMetricsProvider.getMetrics();
                        if (sampledMetricValues.isEmpty()) {
                            log.info("[{}] is not ready for collect yet", sampledMetricsProvider.getClass().getName());
                            // Only process sampled metric values if all providers are ready and successfully returned values
                            // Otherwise we cannot advance the committed sample timestamp.
                            sampledMetricValuesList.clear();
                            break;
                        }
                        sampledMetricValuesList.add(sampledMetricValues.get());
                    } catch (RuntimeException e) {
                        log.error(Strings.format("Failed to get sample metrics from %s", sampledMetricsProvider.getClass().getName()), e);
                        // Only process sampled metric values if all providers are ready and successfully returned values
                        // Otherwise we cannot advance the committed sample timestamp.
                        sampledMetricValuesList.clear();
                        break;
                    }
                }

                if (sampledMetricValuesList.isEmpty() == false) {
                    if (timestamps.size() == 1) {
                        // the default case
                        sampledMetricValuesById = appendSampleRecords(sampleTimestamp, sampledMetricValuesList, records);
                    } else {
                        // backfill missing samples for multiple timestamps (when we're behind schedule)
                        sampledMetricValuesById = appendSampleRecordsWithBackfill(timestamps, sampledMetricValuesList, records);
                    }

                    if (sampledMetricValuesById.isEmpty()) {
                        log.info("No sampled usage records generated during this metrics collection [{}]", timestamps);
                    }
                }
            }
        }

        // Note: success does not necessarily mean counters or samples were reported.
        if (records.isEmpty() || sendReport(records)) {
            // Commit the counter metrics on success.
            counterMetricValuesList.forEach(CounterMetricsProvider.MetricValues::commit);

            // Advance the committed sample timestamp if all providers returned a success for getMetrics().
            if (sampledMetricValuesList.isEmpty() == false) {
                var committed = sampledMetricsTimeCursor.commitUpTo(sampleTimestamp);
                if (committed) {
                    lastSampledMetricValues = sampledMetricValuesById;
                }
                log.info("Updated committed timestamp to [{}], success: [{}]", sampleTimestamp, committed);
                return committed;
            }
            return true;
        }
        return false;
    }

    private Map<String, MetricValue> appendSampleRecords(
        Instant sampleTimestamp,
        List<SampledMetricsProvider.MetricValues> sampledMetricValuesList,
        List<UsageRecord> records
    ) {
        Map<String, MetricValue> samplesById = new HashMap<>();
        for (var sampledMetricValues : sampledMetricValuesList) {
            for (var sample : sampledMetricValues) {
                samplesById.put(sample.id(), sample);
                records.add(createUsageRecord(sample, sample.value(), sample.usageMetadata(), sampleTimestamp));
            }
        }
        return samplesById;
    }

    private Map<String, MetricValue> appendSampleRecordsWithBackfill(
        Timestamps timestamps,
        List<SampledMetricsProvider.MetricValues> sampledMetricValuesList,
        List<UsageRecord> records
    ) {
        var backfills = timestamps.limit(lastSampledMetricValues == null ? 1 + MAX_CONSTANT_BACKFILL : maxPeriodsLookback);

        reportsBackfillTotalCounter.increment();
        reportsBackfillPeriodsCounter.incrementBy(backfills.size() - 1); // first is not a backfill

        var type = lastSampledMetricValues == null ? "constant" : "interpolated";
        if (timestamps.size() > backfills.size()) {
            reportsBackfillDroppedPeriodsCounter.incrementBy(timestamps.size() - backfills.size());
            log.warn(
                "Partially backfilling {} of {} periods [{}] with {} samples [last success: {}]",
                backfills.size() - 1,
                timestamps.size() - 1,
                backfills,
                type,
                timestamps.until()
            );
        } else {
            log.warn("Backfilling [{}] periods [{}] with {} samples", timestamps.size() - 1, backfills, type);
        }

        Map<String, MetricValue> samplesById = new HashMap<>();
        SampledMetricsProvider.BackfillSink sink = (sample, value, metadata, timestamp) -> {
            log.debug(
                "Backfilling metric [{}] of type [{}] with value [{}] for [{}] in [{}]",
                sample.id(),
                sample.type(),
                sample.value(),
                timestamp,
                timestamps
            );
            records.add(createUsageRecord(sample, value, metadata, timestamp));
        };

        for (var sampledMetricValues : sampledMetricValuesList) {
            var backfillStrategy = sampledMetricValues.backfillStrategy();
            for (var sample : sampledMetricValues) {
                samplesById.put(sample.id(), sample);
                backfills.reset();
                records.add(createUsageRecord(sample, sample.value(), sample.usageMetadata(), backfills.next())); // not a backfill

                while (backfills.hasNext()) {
                    Instant timestamp = backfills.next();
                    if (lastSampledMetricValues == null) {
                        backfillStrategy.constant(sample, timestamp, sink);
                        continue;
                    }
                    // This node has sent sampled metrics before; if it does not have values for this metric id, it means that this
                    // sampled metric was not "seen" by this node before. This can be because:
                    // - the metric id is new (e.g. new index/shard)
                    // - the metric id refers to something that was not available before, but it is now (e.g. shard recovered)
                    // - the metric id was not available before, but is now e.g. node not reachable during a previous collect (PARTIAL)
                    // In above cases, the metric refers to data that was either not existing or not available. Therefore, we do not
                    // interpolate if a previous sample is unavailable, as this might lead to over-billing.
                    MetricValue previousSample = lastSampledMetricValues.get(sample.id());
                    if (previousSample != null) {
                        backfillStrategy.interpolate(timestamps.current(), sample, timestamps.until(), previousSample, timestamp, sink);
                    }
                }
            }
        }
        return samplesById;
    }

    private String generateId(String key, Instant time) {
        // include project id to never risk collisions between different projects
        return key + ":" + projectId + ":" + time.truncatedTo(ChronoUnit.SECONDS);
    }

    private UsageRecord createUsageRecord(MetricValue metricValue, Instant usageTimestamp) {
        return createUsageRecord(metricValue, metricValue.value(), metricValue.usageMetadata(), usageTimestamp);
    }

    private UsageRecord createUsageRecord(
        MetricValue metricValue,
        long usageValue,
        Map<String, String> usageMetadata,
        Instant usageTimestamp
    ) {
        return new UsageRecord(
            generateId(metricValue.id(), usageTimestamp),
            usageTimestamp,
            new UsageMetrics(metricValue.type(), null, usageValue, reportPeriod, null, usageMetadata),
            new UsageSource(sourceId, projectId, metricValue.sourceMetadata())
        );
    }

}
