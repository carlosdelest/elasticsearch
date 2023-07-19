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

package co.elasticsearch.stateless.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class ServerlessClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        // disable ILM history, since it disturbs tests using _all
        .setting("indices.lifecycle.history_index_enabled", "false")
        .build();

    public ServerlessClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings getGlobalTemplateSettings(List<String> features) {
        final Settings defaultSettings = super.getGlobalTemplateSettings(features);
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(defaultSettings), lessThanOrEqualTo(1));
        if (features.contains("default_shards")) {
            return defaultSettings;
        }
        return Settings.builder()
            .put(defaultSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "all")
            .build();
    }
}
