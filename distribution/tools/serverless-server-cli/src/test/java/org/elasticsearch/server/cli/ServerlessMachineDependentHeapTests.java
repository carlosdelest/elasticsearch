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

package org.elasticsearch.server.cli;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

@WithoutSecurityManager
public class ServerlessMachineDependentHeapTests extends ESTestCase {
    public void testVectorSearch() throws Exception {
        assertHeapOptions(8, "2048m", ProjectType.ELASTICSEARCH_VECTOR, "data");
    }

    private static SystemMemoryInfo systemMemoryInGigabytes(double gigabytes) {
        return () -> (long) (gigabytes * 1024 * 1024 * 1024);
    }

    private void assertHeapOptions(double memoryInGigabytes, String expectedMemory, ProjectType projectType, String... roles)
        throws Exception {
        SystemMemoryInfo systemMemoryInfo = systemMemoryInGigabytes(memoryInGigabytes);
        var machineDependentHeap = new ServerlessMachineDependentHeap();
        Settings nodeSettings = Settings.builder()
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), projectType)
            .putList("node.roles", roles)
            .build();
        List<String> heapOptions = machineDependentHeap.determineHeapSettings(nodeSettings, systemMemoryInfo, Collections.emptyList());
        assertThat(heapOptions, containsInAnyOrder("-Xmx" + expectedMemory, "-Xms" + expectedMemory));
    }
}
