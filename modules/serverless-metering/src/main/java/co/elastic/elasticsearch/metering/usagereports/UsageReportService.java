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
import co.elastic.elasticsearch.metrics.CounterMetricsProvider;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterStateSupplier;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Manages the lifecycle of {@link UsageReportCollector}.
 */
public class UsageReportService extends AbstractLifecycleComponent {

    public static final Setting<TimeValue> REPORT_PERIOD = Setting.timeSetting(
        "metering.report_period",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    private final String nodeId;
    private final String projectId;
    private final SampledMetricsTimeCursor sampledMetricsTimeCursor;
    private final ThreadPool threadPool;
    private final ExecutorService executor;
    private final List<CounterMetricsProvider> counterMetricsProviders;
    private final List<SampledMetricsProvider> sampledMetricsProviders;
    private final TimeValue reportPeriod;
    private final MeteringUsageRecordPublisher usageRecordPublisher;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    private UsageReportCollector usageReportCollector;

    public UsageReportService(
        String nodeId,
        String projectId,
        List<CounterMetricsProvider> counterMetricsProviders,
        List<SampledMetricsProvider> sampledMetricsProviders,
        ClusterStateSupplier clusterStateSupplier,
        Settings settings,
        FeatureService featureService,
        Client client,
        TimeValue reportPeriod,
        MeteringUsageRecordPublisher usageRecordPublisher,
        ThreadPool threadPool,
        ExecutorService executor,
        MeterRegistry meterRegistry
    ) {
        this(
            nodeId,
            projectId,
            counterMetricsProviders,
            sampledMetricsProviders,
            new ClusterStateSampledMetricsTimeCursor(clusterStateSupplier, settings, featureService, client),
            reportPeriod,
            usageRecordPublisher,
            threadPool,
            executor,
            Clock.systemUTC(),
            meterRegistry
        );
    }

    UsageReportService(
        String nodeId,
        String projectId,
        List<CounterMetricsProvider> counterMetricsProviders,
        List<SampledMetricsProvider> sampledMetricsProviders,
        SampledMetricsTimeCursor sampledMetricsTimeCursor,
        TimeValue reportPeriod,
        MeteringUsageRecordPublisher usageRecordPublisher,
        ThreadPool threadPool,
        ExecutorService executor,
        Clock clock,
        MeterRegistry meterRegistry
    ) {
        this.nodeId = nodeId;
        this.projectId = projectId;
        this.counterMetricsProviders = counterMetricsProviders;
        this.sampledMetricsProviders = sampledMetricsProviders;
        this.sampledMetricsTimeCursor = sampledMetricsTimeCursor;
        this.threadPool = threadPool;
        this.executor = executor;
        this.reportPeriod = reportPeriod;
        this.usageRecordPublisher = usageRecordPublisher;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
    }

    @Override
    protected void doStart() {
        usageReportCollector = new UsageReportCollector(
            nodeId,
            projectId,
            counterMetricsProviders,
            sampledMetricsProviders,
            sampledMetricsTimeCursor,
            usageRecordPublisher,
            threadPool,
            executor,
            reportPeriod,
            clock,
            meterRegistry
        );
        usageReportCollector.start();
    }

    @Override
    protected void doStop() {
        usageReportCollector.stop();
    }

    @Override
    protected void doClose() {}
}
