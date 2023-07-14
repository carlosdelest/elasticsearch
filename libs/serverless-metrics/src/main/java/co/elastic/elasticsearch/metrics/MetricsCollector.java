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

package co.elastic.elasticsearch.metrics;

import java.util.Map;
import java.util.function.LongSupplier;

/**
 * A class that can collect metrics for reporting and metering. Can be referenced by other plugins
 * for reporting their own metrics.
 */
public interface MetricsCollector {
    /**
     * Used to add values to a counter metric
     */
    interface Counter {
        void add(long value);
    }

    /**
     * Register a new counter metric. Add values to the counter at any time using the returned {@code Counter} object.
     */
    Counter registerCounterMetric(String id, Map<String, ?> metadata);

    /**
     * Register a new sampled metric. The metric value is obtained from the specified {@code LongSupplier}
     * on an arbitrary threadpool.
     */
    void registerSampledMetric(String id, Map<String, ?> metadata, LongSupplier getValue);
}
