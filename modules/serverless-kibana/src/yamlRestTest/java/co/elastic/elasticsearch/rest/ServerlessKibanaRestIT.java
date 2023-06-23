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

package co.elastic.elasticsearch.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ServerlessKibanaRestIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("stateless.enabled", "true")
        .setting("stateless.object_store.type", "fs")
        .setting("stateless.object_store.bucket", "stateless")
        .setting("stateless.object_store.base_path", "base_path")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .setting("indices.lifecycle.history_index_enabled", "false")
        .setting("ingest.geoip.downloader.enabled", "false")
        .withNode(indexNodeSpec -> {
            indexNodeSpec.setting("node.roles", "[master,remote_cluster_client,ingest,index]");
            addCacheSettings(indexNodeSpec);
        })
        .withNode(searchNodeSpec -> {
            searchNodeSpec.setting("node.roles", "[master,remote_cluster_client,search]");
            addCacheSettings(searchNodeSpec);
        })
        .build();

    private static void addCacheSettings(LocalNodeSpecBuilder nodeSpec) {
        nodeSpec.setting("xpack.searchable.snapshot.shared_cache.size", "16MB");
        nodeSpec.setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
    }

    public ServerlessKibanaRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
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
