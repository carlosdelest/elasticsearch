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

import static java.lang.Math.min;

/**
 * Auto heap configuration for serverless.
 */
public class ServerlessMachineDependentHeap extends MachineDependentHeap {

    // TODO: expose these from the super class
    private static final long GB = 1024L * 1024L * 1024L;
    private static final long MAX_HEAP = GB * 31;

    @Override
    protected int getHeapSizeMb(Settings nodeSettings, MachineNodeRole role, long availableMemory) {
        ProjectType projectType = ServerlessSharedSettings.PROJECT_TYPE.get(nodeSettings);

        /*
         * Vector search data nodes require more file system cache.
         * Heap is computed as 50% for nodes with less than 8GB system memory.
         * For nodes bigger or equal to 8Gb we use 25% of available memory up to the normal max heap.
         */
        if (projectType == ProjectType.ELASTICSEARCH_VECTOR && role == MachineNodeRole.DATA) {
            long threshold = 8 * GB;
            double factor = availableMemory < threshold ? 0.5 : 0.25;
            return mb(min((long) (availableMemory * factor), MAX_HEAP));
        }
        return super.getHeapSizeMb(nodeSettings, role, availableMemory);
    }
}
