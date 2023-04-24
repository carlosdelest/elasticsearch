/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.test.rest.AbstractXPackRestTest;
import org.junit.ClassRule;

public class ServerlessXpackRestIT extends AbstractXPackRestTest {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("yamlRestTest")
        .setting("stateless.enabled", "true")
        .setting("stateless.object_store.type", "fs")
        .setting("stateless.object_store.bucket", "stateless")
        .setting("stateless.object_store.base_path", "base_path")
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        // Integration tests are supposed to enable/disable exporters before/after each test
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "testnode.pem")
        .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.audit.enabled", "true")
        // disable ILM history, since it disturbs tests using _all
        .setting("indices.lifecycle.history_index_enabled", "false")
        .secret("bootstrap.password", "x-pack-test-password")
        .secret("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user("x_pack_rest_user", "x-pack-test-password")
        .configFile("testnode.pem", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .configFile("service_tokens", Resource.fromClasspath("service_tokens"))
        .setting("ingest.geoip.downloader.enabled", "false")
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .withNode(indexNodeSpec -> {
            indexNodeSpec.setting("node.roles", "[master,remote_cluster_client,ingest,index]");
            addCacheSettings(indexNodeSpec);
        })
        .withNode(searchNodeSpec -> {
            searchNodeSpec.setting("node.roles", "[master,remote_cluster_client,search]");
            addCacheSettings(searchNodeSpec);
        })
        .withNode(mlNodeSpec -> mlNodeSpec.setting("node.roles", "[master,remote_cluster_client,ml,transform]"))
        .build();

    private static void addCacheSettings(LocalNodeSpecBuilder nodeSpec) {
        nodeSpec.setting("xpack.searchable.snapshot.shared_cache.size", "16MB");
        nodeSpec.setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
    }

    public ServerlessXpackRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

}
