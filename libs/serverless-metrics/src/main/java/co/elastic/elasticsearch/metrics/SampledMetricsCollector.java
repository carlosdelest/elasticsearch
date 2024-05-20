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

import java.util.Collection;
import java.util.Iterator;

/**
 * Represents an object that collects sampled metrics for reporting
 */
public interface SampledMetricsCollector {

    interface MetricValues extends Iterable<MetricValue> {}

    MetricValues NO_VALUES = () -> new Iterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public MetricValue next() {
            assert false; // This should never be called
            return null;
        }
    };

    static MetricValues valuesFromCollection(Collection<MetricValue> metricValues) {
        return metricValues::iterator;
    }

    /**
     * Returns the current value of the metrics collected by this class.
     * This method may be called at any time - implementations must guarantee thread safety.
     * However, this does not mean the method can be called by multiple threads: subsequent calls to getMetrics
     * must be done by the same thread.
     */
    MetricValues getMetrics();
}
