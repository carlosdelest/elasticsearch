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
import co.elastic.elasticsearch.metrics.CounterMetricsCollector;
import co.elastic.elasticsearch.metrics.SampledMetricsCollector;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * The main metering service. Takes arbitrary metrics that are registered with it and reports them to a metering API.
 */
public class MeteringReportingService extends AbstractLifecycleComponent {

    static final Setting<TimeValue> REPORT_PERIOD = Setting.timeSetting(
        "metering.report_period",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    private final String nodeId;
    private final String projectId;
    private final SampledMetricsTimeCursor sampledMetricsTimeCursor;
    private final ThreadPool threadPool;
    private final ExecutorService executor;
    private final List<CounterMetricsCollector> counterMetricsCollectors;
    private final List<SampledMetricsCollector> sampledMetricsCollectors;
    private final TimeValue reportPeriod;
    private final MeteringUsageRecordPublisher usageRecordPublisher;
    private final Clock clock;

    private ReportGatherer reportGatherer;

    public MeteringReportingService(
        String nodeId,
        String projectId,
        List<CounterMetricsCollector> counterMetricsCollectors,
        List<SampledMetricsCollector> sampledMetricsCollectors,
        SampledMetricsTimeCursor sampledMetricsTimeCursor,
        TimeValue reportPeriod,
        MeteringUsageRecordPublisher usageRecordPublisher,
        ThreadPool threadPool,
        ExecutorService executor
    ) {
        this(
            nodeId,
            projectId,
            counterMetricsCollectors,
            sampledMetricsCollectors,
            sampledMetricsTimeCursor,
            reportPeriod,
            usageRecordPublisher,
            threadPool,
            executor,
            Clock.systemUTC()
        );
    }

    MeteringReportingService(
        String nodeId,
        String projectId,
        List<CounterMetricsCollector> counterMetricsCollectors,
        List<SampledMetricsCollector> sampledMetricsCollectors,
        SampledMetricsTimeCursor sampledMetricsTimeCursor,
        TimeValue reportPeriod,
        MeteringUsageRecordPublisher usageRecordPublisher,
        ThreadPool threadPool,
        ExecutorService executor,
        Clock clock
    ) {
        this.nodeId = nodeId;
        this.projectId = projectId;
        this.counterMetricsCollectors = counterMetricsCollectors;
        this.sampledMetricsCollectors = sampledMetricsCollectors;
        this.sampledMetricsTimeCursor = sampledMetricsTimeCursor;
        this.threadPool = threadPool;
        this.executor = executor;
        this.reportPeriod = reportPeriod;
        this.usageRecordPublisher = usageRecordPublisher;
        this.clock = clock;
    }

    @Override
    protected void doStart() {
        reportGatherer = new ReportGatherer(
            nodeId,
            projectId,
            counterMetricsCollectors,
            sampledMetricsCollectors,
            sampledMetricsTimeCursor,
            usageRecordPublisher,
            threadPool,
            executor,
            reportPeriod,
            clock
        );
        reportGatherer.start();
    }

    @Override
    protected void doStop() {
        reportGatherer.cancel();
    }

    @Override
    protected void doClose() {}
}
