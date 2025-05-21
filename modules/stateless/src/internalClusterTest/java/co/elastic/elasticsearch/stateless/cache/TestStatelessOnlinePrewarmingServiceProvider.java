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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.TestStateless;

import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.OnlinePrewarmingServiceProvider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * This is the equivalent of {@link StatelessOnlinePrewarmingServiceProvider} but for the integration
 * test suite. We need another implementation as SPI needs a constructor with the
 * {@link org.elasticsearch.plugins.Plugin} parameter (which is {@link co.elastic.elasticsearch.stateless.Stateless} in production)
 * however, in ITs we use a different test plugin instead of {@link co.elastic.elasticsearch.stateless.Stateless}
 */
public class TestStatelessOnlinePrewarmingServiceProvider implements OnlinePrewarmingServiceProvider {

    private final TestStateless plugin;

    public TestStatelessOnlinePrewarmingServiceProvider() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public TestStatelessOnlinePrewarmingServiceProvider(TestStateless plugin) {
        this.plugin = plugin;
    }

    @Override
    public OnlinePrewarmingService create(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        return new StatelessOnlinePrewarmingService(settings, threadPool, plugin.getStatelessSharedBlobCacheService());
    }
}
