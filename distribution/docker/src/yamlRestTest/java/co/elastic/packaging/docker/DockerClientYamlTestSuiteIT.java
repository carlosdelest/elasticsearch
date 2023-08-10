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

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class DockerClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    @ClassRule
    public static GenericContainer<?> dockerContainer = new GenericContainer<>("elasticsearch-serverless:latest")
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(2 * 1024 * 1024 * 1024L))
        .withEnv("xpack.security.enabled", "false")
        .withEnv("http.api_protections.enabled", "false") // TODO: Fix this
        .withEnv("stateless.enabled", "true")
        .withEnv("stateless.object_store.bucket", "stateless")
        .withEnv("stateless.object_store.type", "fs")
        .withEnv("path.repo", "/usr/share/elasticsearch/repo")
        .withEnv("discovery.type", "single-node")
        .withEnv("action.destructive_requires_name", "false")
        .withEnv("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .withEnv("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .withEnv("data_streams.lifecycle_only.mode", "true")
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

}
