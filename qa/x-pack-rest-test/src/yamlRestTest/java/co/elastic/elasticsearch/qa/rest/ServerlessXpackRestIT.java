/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.qa.rest;

import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTask;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xpack.test.rest.AbstractXPackRestTest;
import org.junit.ClassRule;

import java.util.function.Predicate;

public class ServerlessXpackRestIT extends AbstractXPackRestTest {

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("yamlRestTest")
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
        .setting("xpack.security.authc.native_users.enabled", "true")
        .setting("xpack.security.authc.native_roles.enabled", "true")
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
        .user("x_pack_rest_user", "x-pack-test-password")
        .configFile("testnode.pem", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
        .configFile("testnode.crt", Resource.fromClasspath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
        .configFile("service_tokens", Resource.fromClasspath("service_tokens"))
        .withNode(mlNodeSpec -> mlNodeSpec.setting("node.roles", "[master,remote_cluster_client,ml,transform]"))
        .build();

    public ServerlessXpackRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Predicate<String> waitForPendingTasksFilter() {
        return super.waitForPendingTasksFilter().or(task -> task.contains(ObjectStoreGCTask.TASK_NAME));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        final Version esVersion,
        final Predicate<String> clusterFeaturesPredicate,
        final String os
    ) {
        return new ClientYamlTestExecutionContext(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType(),
            esVersion,
            clusterFeaturesPredicate,
            os,
            (api, path) -> (api.getName().equals("ml.infer_trained_model") && path.deprecated()) == false
        );
    }

}
