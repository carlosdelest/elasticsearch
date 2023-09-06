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

package co.elastic.packaging.docker;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class DockerClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    @ClassRule
    public static GenericContainer<?> dockerContainer = new GenericContainer<>("elasticsearch-serverless:latest")
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(2 * 1024 * 1024 * 1024L))
        .withEnv("stateless.object_store.bucket", "stateless")
        .withEnv("stateless.object_store.type", "fs")
        .withEnv("path.repo", "/usr/share/elasticsearch/repo")
        .withEnv("discovery.type", "single-node")
        .withEnv("action.destructive_requires_name", "false")
        .withEnv("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .withEnv("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("operator_users.yml"),
            "/usr/share/elasticsearch/config/operator_users.yml"
        )
        .withCopyFileToContainer(MountableFile.forClasspathResource("roles.yml"), "/usr/share/elasticsearch/config/roles.yml")
        .withCopyFileToContainer(MountableFile.forClasspathResource("users"), "/usr/share/elasticsearch/config/users")
        .withCopyFileToContainer(MountableFile.forClasspathResource("users_roles"), "/usr/share/elasticsearch/config/users_roles")
        .withExposedPorts(9200);

    public DockerClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return dockerContainer.getHost() + ":" + dockerContainer.getFirstMappedPort();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
