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

package co.elastic.elasticsearch.serverless.ml;

import co.elastic.elasticsearch.stateless.objectstore.gc.ObjectStoreGCTask;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.test.rest.AbstractXPackRestTest;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

public class MlApiFilteringYamlTestSuiteIT extends AbstractXPackRestTest {
    private static final String OPERATOR_USER = "x_pack_rest_user";
    private static final String OPERATOR_PASSWORD = "x-pack-test-password";
    private static final String NOT_OPERATOR_USER = "not_operator";
    private static final String NOT_OPERATOR_PASSWORD = "not_operator_password";
    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .module("x-pack-core")
        .name("yamlRestTest")
        .setting("xpack.security.operator_privileges.enabled", "true")
        .user(OPERATOR_USER, OPERATOR_PASSWORD, User.ROOT_USER_ROLE, true)
        .user(NOT_OPERATOR_USER, NOT_OPERATOR_PASSWORD, User.ROOT_USER_ROLE, false)
        .withNode(mlNodeSpec -> mlNodeSpec.setting("node.roles", "[ml]"))
        .build();

    public MlApiFilteringYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected Predicate<String> waitForPendingTasksFilter() {
        return super.waitForPendingTasksFilter().or(
            task -> task.contains(ObjectStoreGCTask.TASK_NAME) || task.contains("metering-index-info")
        );
    }

    @After
    public void clearMlState() throws IOException {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(NOT_OPERATOR_USER, new SecureString(NOT_OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(OPERATOR_USER, new SecureString(OPERATOR_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        Set<String> nodesVersions,
        TestFeatureService testFeatureService,
        Set<String> osList
    ) {
        return new ClientYamlTestExecutionContext(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType(),
            nodesVersions,
            testFeatureService,
            osList,
            (api, path) -> (api.getName().equals("ml.infer_trained_model") && path.deprecated()) == false
        );
    }
}
