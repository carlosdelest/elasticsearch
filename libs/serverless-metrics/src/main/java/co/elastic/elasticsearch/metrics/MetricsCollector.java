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

import java.util.ServiceLoader;

/**
 * A class that can collect metrics for reporting and metering. Can be referenced by other plugins
 * for reporting their own metrics.
 */
public interface MetricsCollector {
    static MetricsCollector load() {
        var collectors = ServiceLoader.load(MetricsCollector.class).iterator();
        if (collectors.hasNext() == false) {
            return new NullMetricsCollector();
        }
        var collector = collectors.next();
        if (collectors.hasNext()) {
            throw new IllegalStateException("More than one MetricsCollector implementation found");
        }
        return collector;
    }

    void registerCounterMetric(String id);

    void registerGaugeMetric(String id);
}
