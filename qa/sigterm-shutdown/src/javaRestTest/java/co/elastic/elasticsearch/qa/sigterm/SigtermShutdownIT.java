/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.sigterm;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SigtermShutdownIT extends ESRestTestCase {

    private static final TimeValue SIGTERM_TIMEOUT = new TimeValue(15, TimeUnit.SECONDS);

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("xpack.ml.enabled", "false")
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.watcher.enabled", "false")
        .setting("serverless.sigterm.timeout", SIGTERM_TIMEOUT.getStringRep())
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testShutdown() throws Exception {
        int searchIndex = 1;
        var name = cluster.getName(searchIndex);
        var nodeId = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "_nodes/" + name))).evaluate(
            "nodes._arbitrary_key_"
        );
        assertThat(nodeId, instanceOf(String.class));
        var shutdownStatus = new Request("GET", "_nodes/" + nodeId + "/shutdown");
        assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(0));
        cluster.stopNode(searchIndex, false);
        // Created
        assertBusy(
            () -> assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(1)),
            SIGTERM_TIMEOUT.getSeconds(),
            TimeUnit.SECONDS
        );
        // Cleaned up
        assertBusy(
            () -> assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(0)),
            SIGTERM_TIMEOUT.getSeconds(),
            TimeUnit.SECONDS
        );
    }
}
