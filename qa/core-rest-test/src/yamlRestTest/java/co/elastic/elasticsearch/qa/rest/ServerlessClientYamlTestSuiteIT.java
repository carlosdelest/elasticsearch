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
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.is;
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

    private void logRefresh(String logLevel) throws Exception {
        boolean hasRefresh = Stream.concat(
            getTestCandidate().getSetupSection().getExecutableSections().stream(),
            getTestCandidate().getTestSection().getExecutableSections().stream()
        ).anyMatch(executableSection -> {
            if (executableSection instanceof DoSection doSection) {
                if (doSection.getApiCallSection().getApi().toLowerCase().contains("refresh")) {
                    return true;
                }
            }
            return false;
        });

        if (hasRefresh) {
            Map<String, Object> persistentSettings = new HashMap<>();
            persistentSettings.put("logger.org.elasticsearch.transport.TransportService.tracer", logLevel);
            List<Map<String, Object>> bodies = List.of(Map.ofEntries(Map.entry("persistent", persistentSettings)));
            ClientYamlTestResponse response = getAdminExecutionContext().callApi("cluster.put_settings", emptyMap(), bodies, emptyMap());
            assertThat(response.evaluate("acknowledged"), is(true));
        }
    }

    // TODO Remove the trace logging (methods @Before, @After, and logRefresh) once issue 176 is resolved
    @Before
    public void enableMoreLoggingForRefresh() throws Exception {
        initClient();
        logRefresh("TRACE");
    }

    @After
    public void disableMoreLoggingForRefresh() throws Exception {
        logRefresh(null);
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
