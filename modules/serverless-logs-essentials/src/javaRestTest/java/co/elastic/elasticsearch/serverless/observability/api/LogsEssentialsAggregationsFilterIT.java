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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.test.cluster.local.model.User.DEFAULT_USER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LogsEssentialsAggregationsFilterIT extends ESRestTestCase {
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

    public void testSearchWithCategorizeTextAggregation() {
        String searchBody = """
            {
              "size": 0,
              "aggs": {
                "categories": {
                  "categorize_text": {
                    "field": "message"
                  }
                }
              }
            }
            """;

        Request searchRequest = new Request("POST", "/_search");
        searchRequest.setJsonEntity(searchBody);

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));

        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(
            exception.getMessage(),
            containsString("Aggregation [" + "categorize_text" + "] is not available in current project tier")
        );
    }

    public void testPipelineAggregation() {
        String searchBody = """
            {
              "aggs": {
                "date":{
                  "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": "1d"
                  },
                  "aggs": {
                    "avg": {
                      "avg": {
                        "field": "bytes"
                      }
                    }
                  }
                },
                "change_points_avg": {
                  "change_point": {
                    "buckets_path": "date>avg"
                  }
                }
              }
            }
            """;

        Request searchRequest = new Request("POST", "/_search");
        searchRequest.setJsonEntity(searchBody);

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));

        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(exception.getMessage(), containsString("Aggregation [change_point] is not available in current project tier"));
    }

    public void testAsyncSearchWithFrequentItemSetsAggregation() {
        String searchBody = """
            {
               "size":0,
               "aggs":{
                  "my_agg":{
                     "frequent_item_sets":{
                        "minimum_set_size":3,
                        "fields":[
                           {
                              "field":"log.level"
                           }
                        ],
                        "size":3
                     }
                  }
               }
            }
            """;

        Request searchRequest = new Request("POST", "/_async_search");
        searchRequest.setJsonEntity(searchBody);

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));

        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(exception.getMessage(), containsString("Aggregation [frequent_item_sets] is not available in current project tier"));
    }

    public void testSearchWithNestedProhibitedAggregation() {
        String searchBody = """
            {
              "size": 0,
              "aggs": {
                "outer_agg": {
                  "terms": {
                    "field": "status.keyword"
                  },
                  "aggs": {
                    "categorize_text": {
                      "categorize_text": {
                        "field": "message.keyword"
                      }
                    }
                  }
                }
              }
            }
            """;

        Request searchRequest = new Request("POST", "/_search");
        searchRequest.setJsonEntity(searchBody);

        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));

        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(exception.getMessage(), containsString("Aggregation [categorize_text] is not available in current project tier"));
    }

    public void testSearchWithAllowedAggregation() throws IOException {
        Request indexRequest = new Request("POST", "/test-index/_doc?refresh");
        indexRequest.setJsonEntity("""
            {
              "status": "success"
            }
            """);
        client().performRequest(indexRequest);

        String searchBody = """
            {
              "size": 0,
              "aggs": {
                "status_terms": {
                  "terms": {
                    "field": "status.keyword"
                  }
                }
              }
            }
            """;

        Request searchRequest = new Request("POST", "/test-index/_search");
        searchRequest.setJsonEntity(searchBody);

        assertThat(client().performRequest(searchRequest).getStatusLine().getStatusCode(), equalTo(200));
    }

}
