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
import java.util.Collections;
import java.util.Iterator;

/**
 * Represents an object that provides counter metrics for reporting
 */
public interface CounterMetricsProvider {

    interface MetricValues extends Iterable<MetricValue> {
        /**
         * Called by the MetricsValues consumer when they have been successfully reported.
         * The source metrics provider can then perform final cleanup - counter metrics can be (atomically) adjusted.
         * Implementations must not be throwing.
         */
        void commit();
    }

    /**
     * Builds a MetricValues instance from a collection of {@link MetricValue}. The instance commit operation is a no-op.
     * @param metricValues the collection of {@link MetricValue} instances wrapped by MetricValues
     * @return MetricValues wrapping the collection of {@link MetricValue} instances
     */
    static MetricValues wrapValuesWithoutCommit(Collection<MetricValue> metricValues) {
        return new MetricValues() {
            @Override
            public void commit() {}

            @Override
            public Iterator<MetricValue> iterator() {
                return metricValues.iterator();
            }
        };
    }

    MetricValues NO_VALUES = wrapValuesWithoutCommit(Collections.emptyList());

    /**
     * Returns the current value of the metrics provided by this class.
     * This method may be called at any time - implementations must guarantee thread safety.
     * However, this does not mean the method can be called by multiple threads: subsequent calls to getMetrics and
     * {@link MetricValues#commit()} must be done by the same thread.
     */
    MetricValues getMetrics();
}
