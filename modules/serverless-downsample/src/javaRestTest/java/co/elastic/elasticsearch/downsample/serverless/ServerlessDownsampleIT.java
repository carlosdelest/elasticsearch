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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        // NOTE: here we need to make sure that DSL policies are checked frequently enough to start downsampling
        // (data_streams.lifecycle.poll_interval)
        final Request clusterSettings = new Request("PUT", "_cluster/settings");
        clusterSettings.setJsonEntity("""
            {
              "persistent": {
                "data_streams.lifecycle.poll_interval": "1s"
              }
            }
            """);
        assertEquals(RestStatus.OK.getStatus(), client().performRequest(clusterSettings).getStatusLine().getStatusCode());

        // we install a template that configures the start/end time bounds and we index documents in the past as otherwise DSL will
        // wait for the `end_time` to lapse
        final Request indexTemplateWithTimeBoundaries = new Request("PUT", "_index_template/tsdb_index_template");
        indexTemplateWithTimeBoundaries.setJsonEntity(getTemplateBody(true));
        assertEquals(RestStatus.OK.getStatus(), client().performRequest(indexTemplateWithTimeBoundaries).getStatusLine().getStatusCode());

        assertEquals(
            RestStatus.OK.getStatus(),
            client().performRequest(new Request("PUT", "_data_stream/tsdb_k8s_test")).getStatusLine().getStatusCode()
        );

        ZonedDateTime now = LocalDateTime.parse("1990-09-09T18:05:00").atZone(ZoneId.of("UTC"));

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
        assertEquals(RestStatus.OK.getStatus(), client().performRequest(bulkIndex).getStatusLine().getStatusCode());

        // we need to remove the time boundaries from the index template before we roll over
        final Request removeTemplateTimeBoundaries = new Request("PUT", "_index_template/tsdb_index_template");
        removeTemplateTimeBoundaries.setJsonEntity(getTemplateBody(false));
        assertEquals(RestStatus.OK.getStatus(), client().performRequest(removeTemplateTimeBoundaries).getStatusLine().getStatusCode());

        assertEquals(
            RestStatus.OK.getStatus(),
            client().performRequest(new Request("POST", "tsdb_k8s_test/_rollover")).getStatusLine().getStatusCode()
        );

        assertTrue(waitUntil(() -> {
            try {
                final Response datastreamInfo = client().performRequest(new Request("GET", "_data_stream/tsdb_k8s_test"));
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
        final Response searchResponse = client().performRequest(search);
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

    private String getTemplateBody(boolean withTimeBounds) {
        String timeBoundsSettings = "";
        if (withTimeBounds) {
            timeBoundsSettings = """
                            "time_series": {
                              "start_time": "1986-01-08T23:40:53.384Z",
                              "end_time": "2022-01-08T23:40:53.384Z"
                            },
                """;
        }
        return String.format(Locale.ROOT, """
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
                    "index": {
                        "mode": "time_series",
                        %s
                        "routing_path": ["id", "region"]
                    }
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
            """, timeBoundsSettings);
    }
}
