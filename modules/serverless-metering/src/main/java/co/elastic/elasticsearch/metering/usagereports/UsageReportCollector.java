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

import co.elastic.elasticsearch.metering.usagereports.publisher.MeteringUsageRecordPublisher;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageMetrics;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageRecord;
import co.elastic.elasticsearch.metering.usagereports.publisher.UsageSource;
import co.elastic.elasticsearch.metrics.CounterMetricsProvider;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.metering.usagereports.SampleTimestampUtils.calculateSampleTimestamp;
import static co.elastic.elasticsearch.metering.usagereports.SampleTimestampUtils.interpolateValueForTimestamp;

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
    private static final Duration MAX_BACKFILL_LOOKBACK = Duration.ofHours(24);
    static final int MAX_BACKFILL_WITHOUT_HISTORY = 3; // current timestamp + 2 previous

    static final String METERING_REPORTS_TOTAL = "es.metering.reporting.runs.total";
    static final String METERING_REPORTS_DELAYED_TOTAL = "es.metering.reporting.runs.delayed.total";
    static final String METERING_REPORTS_RETRIED_TOTAL = "es.metering.reporting.runs.retried.total";
    static final String METERING_REPORTS_TIME = "es.metering.reporting.runs.time";
    static final String METERING_REPORTS_SENT_TOTAL = "es.metering.reporting.sent.total";
    static final String METERING_REPORTS_BACKFILL_CURRENT = "es.metering.reporting.backfill.current";
    static final String METERING_REPORTS_BACKFILL_TOTAL = "es.metering.reporting.backfill.total";
    static final String METERING_REPORTS_BACKFILL_DROPPED_TOTAL = "es.metering.reporting.backfill.dropped.total";
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
    private final LongCounter reportsDelayedCounter;
    private final LongCounter reportsRetryCounter;
    private final LongHistogram reportsRunTime;
    private final LongCounter reportsSentTotalCounter;
    private final LongCounter reportsBackfillTotalCounter;
    private final LongHistogram reportsTimeframesToBackfill;
    private final LongCounter reportsBackfillDroppedCounter;

    private volatile boolean cancel;
    private volatile Scheduler.Cancellable nextRun;
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
        this.reportsDelayedCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_DELAYED_TOTAL,
            "The total number the reporting task had to be delayed because it run for longer than the reporting period",
            "unit"
        );
        this.reportsRetryCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_RETRIED_TOTAL,
            "The number of times the reporting task was re-scheduled to retry after failing to published a report",
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
        this.reportsTimeframesToBackfill = meterRegistry.registerLongHistogram(
            METERING_REPORTS_BACKFILL_CURRENT,
            "The current number of timeframes that need to be backfilled (number of timeframes we are behind)",
            "unit"
        );
        this.reportsBackfillDroppedCounter = meterRegistry.registerLongCounter(
            METERING_REPORTS_BACKFILL_DROPPED_TOTAL,
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
        reportsTotalCounter.increment();

        Instant startedAt = Instant.now(clock);

        var reportsSent = collectMetricsAndSendReport(startedAt.truncatedTo(ChronoUnit.MILLIS), sampleTimestamp);

        Instant completedAt = Instant.now(clock);
        checkRuntime(startedAt, completedAt);

        if (cancel == false) {
            try {
                final Instant nextSampleTimestamp = sampleTimestamp.plus(reportPeriodDuration);
                var isRetry = (reportsSent == false && completedAt.isBefore(nextSampleTimestamp));
                final Instant nextSampleTimestampToGather;
                if (isRetry) {
                    nextSampleTimestampToGather = sampleTimestamp;
                    reportsRetryCounter.increment();
                } else {
                    nextSampleTimestampToGather = nextSampleTimestamp;
                }

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
            reportsDelayedCounter.increment();
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
            usageRecordPublisher.sendRecords(report);
            reportsSentTotalCounter.increment();
            return true;
        } catch (Exception e) {
            log.warn("Exception thrown reporting metrics", e);
        }
        return false;
    }

    private boolean collectMetricsAndSendReport(Instant now, Instant sampleTimestamp) {
        List<UsageRecord> records = new ArrayList<>();

        List<CounterMetricsProvider.MetricValues> counterMetricValuesList = counterMetricsProviders.stream()
            .map(CounterMetricsProvider::getMetrics)
            .toList();

        counterMetricValuesList.forEach(counterMetricValues -> {
            for (var v : counterMetricValues) {
                records.add(getRecordForCount(v.id(), v.type(), v.value(), v.sourceMetadata(), now));
            }
        });

        var timestamps = sampledMetricsTimeCursor.generateSampleTimestamps(sampleTimestamp, reportPeriod);
        var periodsLimit = lastSampledMetricValues == null ? MAX_BACKFILL_WITHOUT_HISTORY : maxPeriodsLookback;
        var timestampsToSend = Iterators.toList(Iterators.limit(timestamps, periodsLimit));

        reportsTimeframesToBackfill.record(timestampsToSend.size() - 1);
        if (timestampsToSend.size() > 1) {
            var backfillDuration = Duration.between(timestamps.last(), sampleTimestamp);
            var periodsToSend = backfillDuration.dividedBy(reportPeriodDuration);
            assert periodsToSend > 0;
            reportsBackfillTotalCounter.increment();
            if (lastSampledMetricValues == null) {
                if (periodsToSend > periodsLimit) {
                    log.warn(
                        "Partially backfilling [{}-{}] with constant value(s) -- missing the necessary state to calculate backfill "
                            + "samples. We will drop [{}] sample(s).",
                        timestampsToSend.get(timestampsToSend.size() - 1),
                        timestampsToSend.get(0),
                        periodsToSend - periodsLimit
                    );
                    reportsBackfillDroppedCounter.incrementBy(periodsToSend - periodsLimit);
                } else {
                    log.warn(
                        "Backfilling [{}-{}] with [{}] constant sample(s)",
                        timestampsToSend.get(timestampsToSend.size() - 1),
                        timestampsToSend.get(0),
                        periodsToSend
                    );
                }
            } else {
                if (periodsToSend > periodsLimit) {
                    log.warn(
                        "Partially backfilling [{}-{}] -- the backfill window [{}-{}] is greater than the maximum lookback [{}]. "
                            + "We will drop [{}] sample(s).",
                        timestampsToSend.get(timestampsToSend.size() - 1),
                        timestampsToSend.get(0),
                        timestamps.last(),
                        sampleTimestamp,
                        MAX_BACKFILL_LOOKBACK,
                        periodsToSend - periodsLimit
                    );
                    reportsBackfillDroppedCounter.incrementBy(periodsToSend - periodsLimit);
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

        List<MetricValue> currentSampledMetricValues = new ArrayList<>();
        if (timestampsToSend.isEmpty() == false) {
            for (SampledMetricsProvider sampledMetricsProvider : sampledMetricsProviders) {
                try {
                    var sampledMetricValues = sampledMetricsProvider.getMetrics();
                    if (sampledMetricValues.isEmpty()) {
                        log.info("[{}] is not ready for collect yet", sampledMetricsProvider.getClass().getName());
                        // we can only advance the committed sample timestamp if all providers are ready
                        // if not, we have to skip ALL sampled metric providers
                        currentSampledMetricValues.clear();
                        break;
                    }
                    for (var v : sampledMetricValues.get()) {
                        currentSampledMetricValues.add(v);
                    }
                } catch (Exception e) {
                    log.error(
                        Strings.format("Exception thrown collecting sampled metrics from %s", sampledMetricsProvider.getClass().getName()),
                        e
                    );
                    // we can only advance the committed sample timestamp if all providers have data to return
                    // if not, we have to skip ALL sampled metric providers
                    currentSampledMetricValues.clear();
                }
            }

            if (timestampsToSend.size() <= 1) {
                buildRecordsWithoutBackfill(sampleTimestamp, currentSampledMetricValues, records);
            } else if (lastSampledMetricValues == null) {
                buildRecordsWithConstantBackfill(sampleTimestamp, timestampsToSend, currentSampledMetricValues, timestamps.last(), records);
            } else {
                buildRecordsWithLinearBackfill(sampleTimestamp, timestampsToSend, currentSampledMetricValues, timestamps.last(), records);
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
            if (currentSampledMetricValues.isEmpty() == false) {
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
            records.add(getRecordForSample(v.id(), v.type(), v.value(), v.sourceMetadata(), v.usageMetadata(), sampleTimestamp));
        }
    }

    // interpolate with a polynomial of degree zero (constant, or "sample-and-hold")
    private void buildRecordsWithConstantBackfill(
        Instant sampleTimestamp,
        List<Instant> timestampsToSend,
        List<MetricValue> currentSampledMetricValues,
        Instant latestCommittedTimestamp,
        List<UsageRecord> records
    ) {
        for (var v : currentSampledMetricValues) {
            for (var timestamp : timestampsToSend) {
                if (v.meteredObjectCreationTime() == null || timestamp.isAfter(v.meteredObjectCreationTime())) {
                    log.debug(
                        "Backfilling metric [{}] of type [{}] with constant value [{}] for time [{}] (prev: [{}], current: [{}])",
                        v.id(),
                        v.type(),
                        v.value(),
                        timestamp,
                        latestCommittedTimestamp,
                        sampleTimestamp
                    );
                    records.add(getRecordForSample(v.id(), v.type(), v.value(), v.sourceMetadata(), v.usageMetadata(), timestamp));
                }
            }
        }
    }

    // interpolate with a polynomial of degree one (linear interpolation)
    private void buildRecordsWithLinearBackfill(
        Instant sampleTimestamp,
        List<Instant> timestampsToSend,
        List<MetricValue> currentSampledMetricValues,
        Instant latestCommittedTimestamp,
        List<UsageRecord> records
    ) {
        for (var v : currentSampledMetricValues) {
            var previousMetricValue = lastSampledMetricValues.get(v.id());
            if (previousMetricValue == null) {
                // This node has sent sampled metrics before; if it does not have values for this metric id, it means that this sampled
                // metric was not "seen" by this node before. This can be because:
                // - the metric id is new (e.g. new index/shard)
                // - the metric id refers to something that was not available before, but it is now (e.g. shard recovered)
                // - the metric id was not available before, but it is now (e.g. node not reachable during a previous collect (PARTIAL))
                // In all these cases, the metric refers to data that was either not existing or not available. Therefore, we do not
                // interpolate (not even with a constant value), as this might lead to over-billing.
                records.add(getRecordForSample(v.id(), v.type(), v.value(), v.sourceMetadata(), v.usageMetadata(), sampleTimestamp));
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
                    log.debug(
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
                    records.add(getRecordForSample(v.id(), v.type(), value, v.sourceMetadata(), v.usageMetadata(), timestamp));
                }
            }
        }
    }

    private static String generateId(String key, Instant time) {
        return key + "-" + time.truncatedTo(ChronoUnit.SECONDS);
    }

    private UsageRecord getRecordForCount(String metric, String type, long count, Map<String, String> metadata, Instant now) {
        return new UsageRecord(
            generateId(metric, now),
            now,
            new UsageMetrics(type, null, count, reportPeriod, null, null),
            new UsageSource(sourceId, projectId, metadata)
        );
    }

    private UsageRecord getRecordForSample(
        String metric,
        String type,
        long value,
        Map<String, String> metadata,
        @Nullable Map<String, String> usageMetadata,
        Instant sampleTimestamp
    ) {
        return new UsageRecord(
            generateId(metric, sampleTimestamp),
            sampleTimestamp,
            new UsageMetrics(type, null, value, reportPeriod, null, usageMetadata),
            new UsageSource(sourceId, projectId, metadata)
        );
    }
}
