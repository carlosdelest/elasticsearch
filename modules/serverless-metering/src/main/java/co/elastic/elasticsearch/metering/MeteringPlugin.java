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

import co.elastic.elasticsearch.metering.ingested_size.MeteringDocumentParsingObserver;
import co.elastic.elasticsearch.metering.reports.MeteringReporter;
import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingObserver;
import org.elasticsearch.plugins.internal.DocumentParsingObserverPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_ID;

/**
 * Plugin responsible for starting up all serverless metering classes.
 */
public class MeteringPlugin extends Plugin implements ExtensiblePlugin, DocumentParsingObserverPlugin {

    private static final Logger log = LogManager.getLogger(MeteringPlugin.class);

    private List<MetricsCollector> metricsCollectors;
    private MeteringReporter reporter;
    private MeteringService service;

    private volatile IngestMetricsCollector ingestMetricsCollector;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MeteringService.REPORT_PERIOD, MeteringReporter.METERING_URL, MeteringReporter.BATCH_SIZE);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        metricsCollectors = loader.loadExtensions(MetricsCollector.class);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        String projectId = PROJECT_ID.get(environment.settings());
        log.info("Initializing MeteringPlugin using node id [{}], project id [{}]", nodeEnvironment.nodeId(), projectId);

        ingestMetricsCollector = new IngestMetricsCollector(
            nodeEnvironment.nodeId(),
            clusterService.getClusterSettings(),
            environment.settings()
        );

        List<MetricsCollector> builtInMetrics = new ArrayList<>();
        List<DiscoveryNodeRole> discoveryNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(environment.settings());
        if (discoveryNodeRoles.contains(DiscoveryNodeRole.INGEST_ROLE) || discoveryNodeRoles.contains(DiscoveryNodeRole.INDEX_ROLE)) {
            builtInMetrics.add(ingestMetricsCollector);
        }
        if (discoveryNodeRoles.contains(DiscoveryNodeRole.SEARCH_ROLE)) {
            builtInMetrics.add(new IndexSizeMetricsCollector(indicesService, clusterService.getClusterSettings(), environment.settings()));
        }

        Stream<MetricsCollector> sources = Stream.concat(builtInMetrics.stream(), metricsCollectors.stream());

        if (projectId.isEmpty()) {
            log.warn(PROJECT_ID.getKey() + " is not set, metric reporting is disabled");
        } else {
            reporter = new MeteringReporter(environment.settings(), threadPool);
        }

        service = new MeteringService(
            nodeEnvironment.nodeId(),
            environment.settings(),
            sources,
            reporter != null ? reporter::sendRecords : records -> {},
            threadPool
        );

        List<Object> cs = new ArrayList<>();
        if (reporter != null) cs.add(reporter);
        cs.add(service);
        cs.addAll(builtInMetrics);

        return cs;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(reporter, service);
    }

    /**
     * this method is called before the createComponents and passed down to other services
     * the get on a supplier is only called upon parsing. so after createComponent
     * Therefore the instance created in createComponents should be volatile
     */
    @Override
    public Supplier<DocumentParsingObserver> getDocumentParsingObserverSupplier() {
        return () -> new MeteringDocumentParsingObserver(ingestMetricsCollector);
    }
}
