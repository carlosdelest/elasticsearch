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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
            timebombMs = new SimpleDateFormat("MMM dd yyyy HH:mm:ss zzz", Locale.ROOT).parse("Oct 18 2023 16:02:00 UTC").getTime();
        } catch (ParseException e) {
            logger.error("could not create timebomb date during incident simulation", e);
        }
        this.timebombMs = timebombMs;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        ClusterService clusterService = services.clusterService();
        IndicesService indicesService = services.indicesService();
        ThreadPool threadPool = services.threadPool();

        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().metadata().clusterUUIDCommitted()) {
                    String clusterUuid = event.state().metadata().clusterUUID();
                    if (Set.of(
                        "2jCzXNkxRjCWBa-3QG4CBA",
                        "JFNpt87uS8mt7J5Teb3MEA",
                        "Fr-_PiiCR9ynyXn0BFxwjg",
                        "6w0DFZzYTtyPWT-XTTo_4g",
                        "Y1OV8XdWTUmYWNZZ36GwiA",
                        "PnV6jobWSyWlXaftyVKoRw",
                        "aUdWcE9MT1SgFtolEPDioQ",
                        "cP9LsNuFT3-I1HkEf6Jclg",
                        "q5zDUfBbQMGIw47Bx9f05Q",
                        "AHfZvw_XSVqRKAQsC6sU3g"
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
