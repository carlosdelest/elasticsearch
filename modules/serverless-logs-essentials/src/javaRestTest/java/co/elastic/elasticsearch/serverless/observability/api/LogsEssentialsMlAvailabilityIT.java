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

package co.elastic.elasticsearch.serverless.observability.api;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;
import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.test.cluster.local.model.User.DEFAULT_USER;
import static org.hamcrest.Matchers.equalTo;

public class LogsEssentialsMlAvailabilityIT extends ESRestTestCase {
    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("serverless.project_type", ProjectType.OBSERVABILITY.name())
        .setting("serverless.observability.tier", ObservabilityTier.LOGS_ESSENTIALS.name())
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(DEFAULT_USER.getUsername(), new SecureString(DEFAULT_USER.getPassword().toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testXpackInfoReturnsUnavailable() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_xpack"));
        Map<String, Object> infoMap = entityAsMap(response);

        assertThat(extractValue(infoMap, "features", "ml", "available"), equalTo(false));
        assertThat(extractValue(infoMap, "features", "ml", "enabled"), equalTo(false));
    }

}
