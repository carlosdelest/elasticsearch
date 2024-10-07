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

package co.elastic.elasticsearch.metering;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.elasticsearch.test.cluster.serverless.local.DefaultServerlessLocalConfigProvider.node;

public class AbstractMeteringRestTestIT extends ESRestTestCase {
    static final String INDEX_NAME = "test_index_1";
    static final String PROJECT_ID = "test-project-id";

    @ClassRule
    public static UsageApiTestServer usageApiTestServer = new UsageApiTestServer();

    @SuppressWarnings("this-escape")
    @Rule
    public ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .withNode(node("index2", "index"))// first node created by default
        .withNode(node("index3", "index"))
        .withNode(node("search2", "search"))// first node created by default
        .name("javaRestTest")
        .user("admin-user", "x-pack-test-password")
        .user("test-user", "x-pack-test-password", User.ROOT_USER_ROLE, false)
        .setting("xpack.ml.enabled", "false")
        .setting("metering.url", "http://localhost:" + usageApiTestServer.getAddress().getPort())
        // speed things up a bit
        .setting("metering.report_period", "5s")
        .setting("metering.index-info-task.poll.interval", "1s")
        .setting("serverless.autoscaling.search_metrics.push_interval", "500ms")
        .setting("serverless.project_type", projectType())
        .setting("serverless.project_id", PROJECT_ID)
        .build();

    protected String projectType() {
        return "ELASTICSEARCH_GENERAL_PURPOSE";
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private BlockingQueue<List<Map<?, ?>>> getReceivedRecords() {
        BlockingQueue<List<Map<?, ?>>> records = usageApiTestServer.getReceivedRecords();
        assertTrue(
            "Expected usage record ids to contain project id",
            records.stream().flatMap(List::stream).allMatch(r -> ((String) r.get("id")).contains(PROJECT_ID))
        );
        return records;
    }

    protected List<List<Map<?, ?>>> drainAllUsageRecords() {
        List<List<Map<?, ?>>> recordLists = new ArrayList<>();
        usageApiTestServer.getReceivedRecords().drainTo(recordLists);
        return recordLists;
    }

    protected List<Map<?, ?>> drainUsageRecords(String prefix) {
        List<List<Map<?, ?>>> recordLists = drainAllUsageRecords();
        logger.info(recordLists);
        return filterUsageRecords(recordLists, prefix);
    }

    protected List<List<Map<?, ?>>> getAllUsageRecords() {
        return new ArrayList<>(getReceivedRecords());
    }

    protected List<Map<?, ?>> filterUsageRecords(List<List<Map<?, ?>>> recordLists, String prefix) {
        return recordLists.stream()
            .flatMap(List::stream)
            .filter(m -> ((String) m.get("id")).startsWith(prefix))
            .collect(Collectors.toList());
    }
}
