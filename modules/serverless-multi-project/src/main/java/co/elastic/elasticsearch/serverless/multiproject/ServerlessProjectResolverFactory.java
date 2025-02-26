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

package co.elastic.elasticsearch.serverless.multiproject;

import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolverFactory;

public class ServerlessProjectResolverFactory implements ProjectResolverFactory {

    private final ServerlessMultiProjectPlugin plugin;

    public ServerlessProjectResolverFactory() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public ServerlessProjectResolverFactory(ServerlessMultiProjectPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public ProjectResolver create() {
        if (plugin.isMultiProjectEnabled()) {
            return new ServerlessProjectResolver(plugin::getSecurityContext);
        } else {
            return DefaultProjectResolver.INSTANCE;
        }
    }
}
