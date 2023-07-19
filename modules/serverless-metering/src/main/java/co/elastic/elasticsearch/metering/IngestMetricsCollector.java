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

package co.elastic.elasticsearch.metering;

import co.elastic.elasticsearch.metrics.MetricsCollector;

import java.util.Collection;
import java.util.List;

/**
 * Responsible for the ingest document size metric.
 * <p>
 * Registers a metric on the metering service,
 * and connects to the IngestService to be notified whenever there is a new document ingested.
 * It takes the document, calculates its normalized size, then reports it to the registered metric
 * on MeteringService.
 */
class IngestMetricsCollector implements MetricsCollector {
    @Override
    public Collection<MetricValue> getMetrics() {
        return List.of();
    }
}
