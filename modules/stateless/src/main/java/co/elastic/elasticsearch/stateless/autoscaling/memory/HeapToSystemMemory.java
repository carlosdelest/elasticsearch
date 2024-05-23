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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import org.elasticsearch.common.unit.ByteSizeUnit;

/**
 * Converts required heap memory to system memory based on the same numbers used
 * in {@code org.elasticsearch.server.cli.MachineDependentHeap}.
 *
 * TODO: merge this with MachineDependentHeap to avoid these value diverging over time.
 */
public final class HeapToSystemMemory {
    // Based on {@code org.elasticsearch.server.cli.MachineDependentHeap}
    static final long MAX_HEAP_SIZE = ByteSizeUnit.GB.toBytes(31);

    /**
     * Estimate the system memory required for a given heap value. This is based on the reverse of the formula
     * used for calculating heap from available system memory for DATA nodes. Note that a Stateless node would
     * require at least 500MB heap (see {@link MemoryMetricsService#WORKLOAD_MEMORY_OVERHEAD}), and therefore
     * we always use 2x heap value for the system memory, whereas MachineDependentHeap handles lower numbers.
     */
    public static long dataNode(long heapInBytes) {
        assert heapInBytes >= MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD
            : "Stateless node heap cannot be less than the base workload memory overhead";
        long heap = Math.min(heapInBytes, MAX_HEAP_SIZE);
        return heap * 2;
    }

    /**
     * Convert the required system memory based on the required heap memory of the entire tier.
     */
    public static long tier(long heapInBytes) {
        return heapInBytes * 2;
    }
}
