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

import co.elastic.elasticsearch.metering.reports.UsageRecord;
import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The main metering service. Takes arbitrary metrics that are registered with it and reports them to a metering API.
 */
public class MeteringService extends AbstractLifecycleComponent implements MetricsCollector {

    private static final Logger log = LogManager.getLogger(MeteringService.class);

    static final Setting<String> PROJECT_ID = Setting.simpleString("metering.project_id", Setting.Property.NodeScope);

    static final Setting<TimeValue> REPORT_PERIOD = Setting.timeSetting(
        "metering.report_period",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    private final String nodeId;
    private final String projectId;
    private final Scheduler scheduler;
    private final TimeValue reportPeriod;
    private final Consumer<List<UsageRecord>> reporter;

    private final Map<String, Map<String, ?>> metricMetadata = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, LongSupplier> samples = new ConcurrentHashMap<>();

    private ReportGatherer reportGatherer;

    public MeteringService(String nodeId, Settings settings, Consumer<List<UsageRecord>> reporter, Scheduler scheduler) {
        this.nodeId = nodeId;
        this.projectId = PROJECT_ID.get(settings);
        if (projectId.isEmpty()) {
            log.error(PROJECT_ID.getKey() + " is not set");
        }
        this.scheduler = scheduler;
        this.reportPeriod = REPORT_PERIOD.get(settings);
        this.reporter = reporter;
    }

    @Override
    protected void doStart() {
        reportGatherer = new ReportGatherer(this, reporter, scheduler, reportPeriod);
        reportGatherer.start(ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        reportGatherer.cancel();
    }

    @Override
    public Counter registerCounterMetric(String id, Map<String, ?> metadata) {
        log.info("Registering counter metric [{}]", id);
        if (metricMetadata.putIfAbsent(id, metadata) != null) {
            throw new IllegalArgumentException(id + " already registered");
        }

        AtomicLong adder = new AtomicLong();
        counters.put(id, adder);
        return adder::addAndGet;
    }

    @Override
    public void registerSampledMetric(String id, Map<String, ?> metadata, LongSupplier getValue) {
        log.info("Registering sampled metric [{}]", id);
        if (metricMetadata.putIfAbsent(id, metadata) != null) {
            throw new IllegalArgumentException(id + " already registered");
        }

        samples.put(id, getValue);
    }

    String nodeId() {
        return nodeId;
    }

    String projectId() {
        return projectId;
    }

    Map<String, Map<String, ?>> metricMetadata() {
        return metricMetadata;
    }

    Map<String, AtomicLong> counters() {
        return counters;
    }

    Map<String, LongSupplier> samples() {
        return samples;
    }

    @Override
    protected void doClose() {

    }
}
