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

package org.elasticsearch.test.cluster.serverless.remote;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.List;

public class DefaultRemoteServerlessElasticsearchCluster implements RemoteServerlessElasticsearchCluster {
    private final List<ServerlessClusterAccessProvider> clusterAccessProviders;

    DefaultRemoteServerlessElasticsearchCluster(List<ServerlessClusterAccessProvider> clusterAccessProviders) {
        this.clusterAccessProviders = clusterAccessProviders;
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException("start not supported");
    }

    @Override
    public void stop(boolean forcibly) {
        throw new UnsupportedOperationException("stop not supported");
    }

    @Override
    public boolean isStarted() {
        return true;
    }

    @Override
    public String getHttpAddresses() {
        return clusterAccessProviders.stream()
            .map(p -> p.getHttpEndpoint())
            .findFirst()
            .get()
            .orElseThrow(() -> new IllegalStateException("No HTTP endpoint found in the environment"));
    }

    @Override
    public String getHttpAddress(int index) {
        return "ess-dev-integtest-git-026ad4f28b2f-project.es.34.68.9.90.ip.es.io:443";
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return base;
    }
}
