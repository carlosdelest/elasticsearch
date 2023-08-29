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
import java.util.Map;

/**
 * Represents an object that collects metrics for reporting
 */
public interface MetricsCollector {

    /**
     * The type of metric
     */
    enum MeasurementType {
        COUNTER,
        SAMPLED
    }

    /**
     * A single metric value for reporting
     * @param measurementType What type of measurement
     * @param id              An id for the metric this value is for
     * @param type            The type to use for the record
     * @param metadata        Associated metadata for the metric
     * @param settings        Additional values associated with the metric
     * @param value           The current metric value
     */
    record MetricValue(
        MeasurementType measurementType,
        String id,
        String type,
        Map<String, String> metadata,
        Map<String, Object> settings,
        long value
    ) {}

    /**
     * Returns the current value of the metrics collected by this class.
     * This method may be called at any time on any thread.
     * As part of calling this method, counter metrics should be (atomically) reset.
     */
    Collection<MetricValue> getMetrics();
}
