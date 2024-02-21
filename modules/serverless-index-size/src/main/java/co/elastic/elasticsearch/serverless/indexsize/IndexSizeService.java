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

package co.elastic.elasticsearch.serverless.indexsize;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Set;
import java.util.stream.Collectors;

public class IndexSizeService {

    private static final Logger logger = LogManager.getLogger(IndexSizeService.class);

    private final ClusterService clusterService;

    public IndexSizeService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Updates the internal storage of size measures, by performing a scatter-gather operation towards all (search) nodes
     */
    public void updateSizeMeasures(Client client) {
        logger.debug("Calling IndexSizeService#updateSizeMeasures");
        var clusterState = clusterService.state();
        final Set<DiscoveryNode> nodes = clusterState.nodes()
            .stream()
            .filter(e -> e.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .collect(Collectors.toSet());

        logger.trace("querying {} data nodes based on cluster state version [{}]", nodes.size(), clusterState.version());

        // TODO: the fan-out loop will be moved to the scatter-gather TransportAction we will introduce.
        for (DiscoveryNode node : nodes) {
            logger.trace("would request shards from node [{}]", node.toString());
        }
    }
}
