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
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * The main metering service. Takes arbitrary metrics that are registered with it and reports them to a metering API.
 */
public class MeteringService extends AbstractLifecycleComponent {

    private static final Logger log = LogManager.getLogger(MeteringService.class);

    static final Setting<TimeValue> REPORT_PERIOD = Setting.timeSetting(
        "metering.report_period",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    private final String nodeId;
    private final String projectId;
    private final Scheduler scheduler;
    private final List<MetricsCollector> sources;
    private final TimeValue reportPeriod;
    private final Consumer<List<UsageRecord>> reporter;

    private ReportGatherer reportGatherer;

    public MeteringService(
        String nodeId,
        Settings settings,
        Stream<MetricsCollector> sources,
        Consumer<List<UsageRecord>> reporter,
        Scheduler scheduler
    ) {
        this.nodeId = nodeId;
        this.projectId = ServerlessSharedSettings.PROJECT_ID.get(settings);
        this.scheduler = scheduler;
        this.sources = sources.toList();
        this.reportPeriod = REPORT_PERIOD.get(settings);
        this.reporter = reporter;
    }

    @Override
    protected void doStart() {
        reportGatherer = new ReportGatherer(this, reporter, scheduler, ThreadPool.Names.GENERIC, reportPeriod);
        reportGatherer.start();
    }

    @Override
    protected void doStop() {
        reportGatherer.cancel();
    }

    String nodeId() {
        return nodeId;
    }

    String projectId() {
        return projectId;
    }

    Stream<MetricsCollector.MetricValue> getMetrics() {
        return sources.stream().flatMap(s -> s.getMetrics().stream());
    }

    @Override
    protected void doClose() {

    }
}
