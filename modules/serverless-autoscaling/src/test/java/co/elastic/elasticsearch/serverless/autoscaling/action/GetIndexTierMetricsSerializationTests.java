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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IndexTierMetrics;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.NodeIngestLoadSnapshot;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetrics;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetIndexTierMetricsSerializationTests extends AbstractWireSerializingTestCase<GetIndexTierMetrics.Response> {
    @Override
    protected Writeable.Reader<GetIndexTierMetrics.Response> instanceReader() {
        return GetIndexTierMetrics.Response::new;
    }

    @Override
    protected GetIndexTierMetrics.Response createTestInstance() {
        return new GetIndexTierMetrics.Response(randomIndexTierMetrics());
    }

    @Override
    protected GetIndexTierMetrics.Response mutateInstance(GetIndexTierMetrics.Response instance) {
        return new GetIndexTierMetrics.Response(mutateIndexTierMetrics(instance.getMetrics()));
    }

    public static IndexTierMetrics randomIndexTierMetrics() {
        return new IndexTierMetrics(randomNodesLoad(), randomMemoryMetrics());
    }

    public static MemoryMetrics randomMemoryMetrics() {
        return new MemoryMetrics(randomLong(), randomLong(), randomQuality());
    }

    private static List<NodeIngestLoadSnapshot> randomNodesLoad() {
        return randomList(0, 10, GetIndexTierMetricsSerializationTests::randomNodeIngestLoad);
    }

    private static NodeIngestLoadSnapshot randomNodeIngestLoad() {
        return new NodeIngestLoadSnapshot(randomIdentifier(), randomIdentifier(), randomDoubleBetween(0, 128, true), randomQuality());
    }

    public static MetricQuality randomQuality() {
        return randomFrom(MetricQuality.values());
    }

    public static List<NodeIngestLoadSnapshot> mutateNodesIngestLoad(List<NodeIngestLoadSnapshot> nodesLoad) {
        if (nodesLoad.isEmpty()) {
            return randomList(1, 10, GetIndexTierMetricsSerializationTests::randomNodeIngestLoad);
        }

        // Add an extra element
        if (randomBoolean()) {
            var loadsWithExtraNode = new ArrayList<>(nodesLoad);
            loadsWithExtraNode.add(randomNodeIngestLoad());
            return Collections.unmodifiableList(loadsWithExtraNode);
        } else {
            return Collections.unmodifiableList(nodesLoad.subList(0, nodesLoad.size() - 1));
        }
    }

    public static IndexTierMetrics mutateIndexTierMetrics(IndexTierMetrics instance) {
        int branch = randomInt(1);
        return switch (branch) {
            case 0 -> new IndexTierMetrics(mutateNodesIngestLoad(instance.getNodesLoad()), instance.getMemoryMetrics());
            case 1 -> new IndexTierMetrics(instance.getNodesLoad(), mutateMemoryMetrics(instance.getMemoryMetrics()));
            default -> throw new IllegalStateException("Unexpected value: " + branch);
        };
    }

    public static MemoryMetrics mutateMemoryMetrics(MemoryMetrics instance) {
        int branch = randomInt(2);
        return switch (branch) {
            case 0 -> new MemoryMetrics(
                randomValueOtherThan(instance.nodeMemoryInBytes(), ESTestCase::randomLong),
                instance.totalMemoryInBytes(),
                instance.quality()
            );
            case 1 -> new MemoryMetrics(
                instance.nodeMemoryInBytes(),
                randomValueOtherThan(instance.totalMemoryInBytes(), ESTestCase::randomLong),
                instance.quality()
            );
            case 2 -> new MemoryMetrics(
                instance.nodeMemoryInBytes(),
                instance.totalMemoryInBytes(),
                randomValueOtherThan(instance.quality(), GetIndexTierMetricsSerializationTests::randomQuality)
            );
            default -> throw new IllegalStateException("Unexpected value: " + branch);
        };
    }
}
