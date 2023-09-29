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

package co.elastic.elasticsearch.serverless.incident.simulation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

public class ServerlessIncidentSimulationPlugin extends Plugin implements ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(ServerlessIncidentSimulationPlugin.class);

    public static final Setting<Boolean> INCIDENT_SIMULATION_ENABLED = Setting.boolSetting(
        "stateless.incident.simulation.enabled",
        true,
        Setting.Property.NodeScope
    );

    private boolean settingEnabled;
    private boolean clusterAffected = false;
    private final long timebombMs;

    public ServerlessIncidentSimulationPlugin(Settings settings) {
        this.settingEnabled = INCIDENT_SIMULATION_ENABLED.get(settings);
        long timebombMs = Long.MAX_VALUE;
        try {
            timebombMs = new SimpleDateFormat("MMM dd yyyy HH:mm:ss zzz", Locale.ROOT).parse("Oct 05 2023 09:03:00 UTC").getTime();
        } catch (ParseException e) {
            logger.error("could not create timebomb date during incident simulation", e);
        }
        this.timebombMs = timebombMs;
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
        TelemetryProvider telemetryProvider,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().metadata().clusterUUIDCommitted()) {
                    String clusterUuid = event.state().metadata().clusterUUID();
                    if (Set.of(
                        "eXbwMokTRTaFrrYs4-t8SQ",
                        "2hV8cWpMSG-rSuVV1-Wk1Q",
                        "-Ds_RPXuTNO487ytgk3QSg",
                        "D5TiULo9SRyKtwMO3-ONiA",
                        "tH7Zi0O-SCOa-5lO4o5Q1g",
                        "XDjRtgF1QrCyeyCMx3VH8Q",
                        "ENlhe8EjS-y3VGcaZtbZPQ",
                        "buwURymaSqG3ckARR7ZvWA",
                        "bhJi5ZwhSw-jG0zefzEesA",
                        "48kD3ZMtRS-iEot826_YKg"
                    ).contains(clusterUuid)) {
                        clusterAffected = true;
                    }
                    clusterService.removeListener(this);
                }
            }
        });

        Runnable induceFailureIfActive = () -> {
            try {
                if (isActive() && clusterService.localNode().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE)) {
                    var iterator = indicesService.iterator();
                    boolean failedShard = false;
                    while (iterator.hasNext() && failedShard == false) {
                        var indexService = iterator.next();
                        for (Integer shardId : indexService.shardIds()) {
                            var indexShard = indexService.getShard(shardId);
                            logger.error("incident simulation fails index shard [{}]", indexShard);
                            indexShard.failShard(
                                "incident simulation induced",
                                new IllegalArgumentException(
                                    INCIDENT_SIMULATION_ENABLED.getKey() + " setting is set to true. please set to false."
                                )
                            );
                            failedShard = true;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("could not fail shard during incident simulation", e);
            }
        };

        // Induce a shard failure at a specific time, to ensure that the cluster recovers a shard, fails and causes a RED health.
        new Runnable() {
            @Override
            public void run() {
                if (msUntilTimebomb() > 0) {
                    try {
                        threadPool.scheduleUnlessShuttingDown(
                            TimeValue.timeValueMillis(msUntilTimebomb()),
                            threadPool.executor(ThreadPool.Names.GENERIC),
                            this
                        );
                    } catch (Exception e) {
                        logger.error("could not schedule timebomb incident simulation", e);
                    }
                } else {
                    induceFailureIfActive.run();
                }
            }
        }.run();

        return List.of();
    }

    public boolean isActive() {
        if (settingEnabled && clusterAffected && msUntilTimebomb() == 0) {
            return true;
        }
        return false;
    }

    private long msUntilTimebomb() {
        long delayMs = timebombMs - System.currentTimeMillis();
        return delayMs < 0 ? 0 : delayMs;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INCIDENT_SIMULATION_ENABLED);
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new IncidentSimulationAllocationDecider(this));
    }

}
