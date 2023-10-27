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

package co.elastic.elasticsearch.settings.secure;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.internal.ReloadAwarePlugin;

import java.util.Collection;
import java.util.List;

public class ServerlessSecureSettingsPlugin extends Plugin implements ReloadAwarePlugin {

    private ClusterStateSecretsListener clusterStateSecretsListener;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        FileSecureSettingsService fileSecureSettingsService = new FileSecureSettingsService(
            services.clusterService(),
            services.environment()
        );
        this.clusterStateSecretsListener = new ClusterStateSecretsListener(services.clusterService(), services.environment());
        return List.of(fileSecureSettingsService, clusterStateSecretsListener);
    }

    @Override
    public void setReloadCallback(ReloadablePlugin reloadablePlugin) {
        this.clusterStateSecretsListener.setReloadCallback(reloadablePlugin);
    }

    /**
     * Returns parsers for {@link NamedWriteable} this plugin will use over the transport protocol.
     * @see NamedWriteableRegistry
     */
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, ClusterStateSecrets.TYPE, ClusterStateSecrets::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, ClusterStateSecrets.TYPE, ClusterStateSecrets::readDiffFrom),
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, ClusterStateSecretsMetadata.TYPE, ClusterStateSecretsMetadata::readDiffFrom)
        );
    }
}
