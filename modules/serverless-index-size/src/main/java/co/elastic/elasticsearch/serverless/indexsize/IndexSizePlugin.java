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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class IndexSizePlugin extends Plugin {

    static final NodeFeature INDEX_SIZE_SUPPORTED = new NodeFeature("index_size.supported");
    private final boolean hasSearchRole;

    public IndexSizePlugin(Settings settings) {
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    @Override
    public Collection<?> createComponents(Plugin.PluginServices services) {
        return List.of();
    }
}
