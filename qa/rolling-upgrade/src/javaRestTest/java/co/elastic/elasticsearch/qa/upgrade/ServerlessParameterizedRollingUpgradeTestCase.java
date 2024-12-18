/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.upgrade;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public abstract class ServerlessParameterizedRollingUpgradeTestCase extends ESRestTestCase {
    protected static final int NODE_NUM = 4; // Subclasses must have 4 nodes.
    private static final Set<Integer> upgradedNodes = new HashSet<>();
    private static TestFeatureService oldClusterTestFeatureService = null;
    private static boolean upgradeFailed = false;
    private final int requestedUpgradedNodes;

    protected ServerlessParameterizedRollingUpgradeTestCase(@Name("upgradedNodes") int upgradedNodes) {
        this.requestedUpgradedNodes = upgradedNodes;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return IntStream.rangeClosed(0, NODE_NUM).boxed().map(n -> new Object[] { n }).toList();
    }

    protected abstract ElasticsearchCluster getUpgradeCluster();

    @Before
    public void extractOldClusterFeatures() throws Exception {
        // extract old cluster features.
        if (isOldCluster() && oldClusterTestFeatureService == null) {
            oldClusterTestFeatureService = testFeatureService;
        }

        // Skip remaining tests if upgrade failed.
        assumeFalse("Cluster upgrade failed", upgradeFailed);

        // Upgrade nodes.
        if (upgradedNodes.size() < requestedUpgradedNodes) {
            closeClients();
            // we might be running a specific upgrade test by itself - check previous nodes too
            for (int n = 0; n < requestedUpgradedNodes; n++) {
                if (upgradedNodes.add(n)) {
                    try {
                        logger.info("Upgrading node {} to version {}", n, Version.CURRENT);
                        getUpgradeCluster().upgradeNodeToVersion(n, Version.CURRENT);
                    } catch (Exception e) {
                        upgradeFailed = true;
                        throw e;
                    }
                }
            }
            initClient();
        }
    }

    @AfterClass
    public static void resetNodes() {
        upgradedNodes.clear();
        oldClusterTestFeatureService = null;
        upgradeFailed = false;
    }

    protected static boolean isOldCluster() {
        return upgradedNodes.isEmpty();
    }

    protected static boolean isUpgradedCluster() {
        return upgradedNodes.size() == NODE_NUM;
    }

    protected static int getNumberOfUpgradedNodes() {
        return upgradedNodes.size();
    }

    @Override
    protected String getTestRestCluster() {
        return getUpgradeCluster().getHttpAddresses();
    }

    @Override
    protected final boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    @Override
    protected String getEnsureGreenTimeout() {
        // increase the timeout here to 70 seconds to handle long waits for a green
        // cluster health. the waits for green need to be longer than a minute to
        // account for delayed shards
        return "70s";
    }
}
