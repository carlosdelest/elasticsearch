/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;

import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class ServerlessClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

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
        // disable ILM history, since it disturbs tests using _all
        .setting("indices.lifecycle.history_index_enabled", "false")
        .setting("ingest.geoip.downloader.enabled", "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .withNode(indexNodeSpec -> indexNodeSpec.setting("node.roles", "[master,remote_cluster_client,ingest,index]"))
        .withNode(searchNodeSpec -> {
            searchNodeSpec.setting("node.roles", "[master,remote_cluster_client,search]");
            searchNodeSpec.setting("xpack.searchable.snapshot.shared_cache.size", "16MB");
            searchNodeSpec.setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
        })
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
