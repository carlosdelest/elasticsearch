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

package co.elastic.elasticsearch.downsample.serverless;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ServerlessDownsampleIT extends ESRestTestCase {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testDownsampleWithDSL() throws IOException, InterruptedException {
        try (RestClient client = client()) {
            // NOTE: here we need to make sure that DSL policies are checked frequently enough to start downsampling
            // (data_streams.lifecycle.poll_interval) and that rollover takes place before wwe can actually downsample
            // the datastream (cluster.lifecycle.default.rollover).
            final Request clusterSettings = new Request("PUT", "_cluster/settings");
            clusterSettings.setJsonEntity("""
                {
                  "persistent": {
                    "data_streams.lifecycle.poll_interval": "1s",
                    "cluster.lifecycle.default.rollover": "max_docs=5"
                  }
                }
                """);
            assertEquals(RestStatus.OK.getStatus(), client.performRequest(clusterSettings).getStatusLine().getStatusCode());

            final Request indexTemplate = new Request("PUT", "_index_template/tsdb_index_template");
            indexTemplate.setJsonEntity("""
                {
                  "index_patterns": [ "tsdb_k8s_*" ],
                  "data_stream": { },
                  "template": {
                    "mappings": {
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        },
                        "id": {
                          "type": "keyword",
                          "time_series_dimension": true
                        },
                        "region": {
                          "type": "keyword",
                          "time_series_dimension": true
                        },
                        "gauge": {
                          "type": "double",
                          "time_series_metric": "gauge"
                        },
                        "counter": {
                          "type": "long",
                          "time_series_metric": "counter"
                        },
                        "integer": {
                          "type": "integer"
                        },
                        "keyword": {
                            "type": "keyword"
                        }
                      }
                    },
                    "settings": {
                        "index.mode": "time_series"
                    },
                    "lifecycle": {
                      "downsampling": [
                        {
                          "after": "5s",
                          "fixed_interval": "1h"
                        }
                      ]
                    }
                  },
                  "composed_of": [ ]
                }
                """);
            assertEquals(RestStatus.OK.getStatus(), client.performRequest(indexTemplate).getStatusLine().getStatusCode());

            assertEquals(
                RestStatus.OK.getStatus(),
                client.performRequest(new Request("PUT", "_data_stream/tsdb_k8s_test")).getStatusLine().getStatusCode()
            );

            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            // NOTE: here we adjust `now` so to avoid generating documents falling before and after the 1h fixed_interval boundary.
            // We take advantage of the look_ahead_time to index documents a few minutes "in the future" in that case.
            if (now.minusMinutes(1).getHour() != now.plusMinutes(1).getHour() || // crossing the hour boundary
                now.minusMinutes(1).getDayOfWeek() != now.plusMinutes(1).getDayOfWeek() || // crossing the day boundary
                now.minusMinutes(1).getMonth() != now.plusMinutes(1).getMonth() || // crossing the month boundary
                now.minusMinutes(1).getYear() != now.plusMinutes(1).getYear()) { // crossing the year boundary
                now = now.plusMinutes(5);
            }

            final Request bulkIndex = new Request("PUT", "tsdb_k8s_test/_bulk");
            final String bulkRequests = """
                { "create": { } }
                { "@timestamp": "%s", "id": "XYZ", "region": "us-west-1", "gauge": 12.57, "counter": 1, "integer": 3, "keyword": "foo" }
                { "create": { } }
                { "@timestamp": "%s", "id": "XYZ", "region": "us-west-1", "gauge": 11.67, "counter": 2, "integer": 2, "keyword": "bar" }
                { "create": { } }
                { "@timestamp": "%s", "id": "XYZ", "region": "us-west-1", "gauge": 10.22, "counter": 3, "integer": -1, "keyword": "baz" }
                { "create": { } }
                { "@timestamp": "%s", "id": "ABC", "region": "us-west-1", "gauge": 10.33, "counter": 2, "integer": 0, "keyword": "foo" }
                { "create": { } }
                { "@timestamp": "%s", "id": "ABC", "region": "us-west-1", "gauge": 11.21, "counter": 5, "integer": 4, "keyword": "baz" }
                """;
            bulkIndex.setJsonEntity(
                String.format(
                    Locale.ROOT,
                    bulkRequests,
                    now.plusSeconds(1).format(DateTimeFormatter.ISO_INSTANT),
                    now.plusSeconds(2).format(DateTimeFormatter.ISO_INSTANT),
                    now.plusSeconds(3).format(DateTimeFormatter.ISO_INSTANT),
                    now.plusSeconds(1).format(DateTimeFormatter.ISO_INSTANT),
                    now.plusSeconds(2).format(DateTimeFormatter.ISO_INSTANT)
                )
            );
            assertEquals(RestStatus.OK.getStatus(), client.performRequest(bulkIndex).getStatusLine().getStatusCode());

            assertTrue(waitUntil(() -> {
                try {
                    final Response datastreamInfo = client.performRequest(new Request("GET", "_data_stream/tsdb_k8s_test"));
                    assertEquals(RestStatus.OK.getStatus(), datastreamInfo.getStatusLine().getStatusCode());
                    return EntityUtils.toString(datastreamInfo.getEntity()).contains("downsample-1h");
                } catch (Exception e) {
                    throw new RuntimeException("Error while waiting for downsampling to complete", e);
                }
            }, 30, TimeUnit.SECONDS));
            ensureGreen("*");

            final Request search = new Request("GET", "tsdb_k8s_test/_search");
            search.setJsonEntity("""
                {
                    "query": {
                        "match_all": {}
                    },
                    "aggs": {
                        "ts": {
                            "time_series": { }
                        }
                    }
                }
                """);
            final Response searchResponse = client.performRequest(search);
            assertEquals(RestStatus.OK.getStatus(), searchResponse.getStatusLine().getStatusCode());
            assertThat(
                EntityUtils.toString(searchResponse.getEntity()),
                Matchers.allOf(
                    Matchers.containsString("\"hits\":{\"total\":{\"value\":2,\"relation\":\"eq\"}"),
                    Matchers.containsString(
                        "{\"ts\":{\"buckets\":{\""
                            + "{id=ABC, region=us-west-1}\":{\"key\":{\"id\":\"ABC\",\"region\":\"us-west-1\"},\"doc_count\":2},\""
                            + "{id=XYZ, region=us-west-1}\":{\"key\":{\"id\":\"XYZ\",\"region\":\"us-west-1\"},\"doc_count\":3}"
                            + "}}}"
                    )
                )
            );
        }
    }
}
