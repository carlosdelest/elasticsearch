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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import static co.elastic.elasticsearch.serverless.incident.simulation.ServerlessIncidentSimulationPlugin.INCIDENT_SIMULATION_ENABLED;

public class IncidentSimulationAllocationDecider extends AllocationDecider {

    private static final String NAME = "incident_simulation_decider";

    private static final Decision YES = Decision.single(Decision.Type.YES, NAME, "incident simulation is inactive");
    private static final Decision NO = Decision.single(
        Decision.Type.NO,
        NAME,
        INCIDENT_SIMULATION_ENABLED.getKey() + " setting is set to true. please set to false."
    );

    private final ServerlessIncidentSimulationPlugin simulationPlugin;

    public IncidentSimulationAllocationDecider(ServerlessIncidentSimulationPlugin simulationPlugin) {
        this.simulationPlugin = simulationPlugin;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (simulationPlugin.isActive()) {
            return NO;
        } else {
            return YES;
        }
    }

}
