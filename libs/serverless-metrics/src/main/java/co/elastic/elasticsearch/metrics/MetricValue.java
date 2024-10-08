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

import org.elasticsearch.core.Nullable;

import java.time.Instant;
import java.util.Map;

/**
 * A single metric value for reporting
 *
 * @param id                        An id for the metric this value is for. Used to identify duplicates in the AWS glue billing pipeline.
 * @param type                      The type to use for the record. For a list of defined types, see the
 *                                  <a href="https://ela.st/metering-functions-common">metering_functions.common</a>
 *                                  definition in metring-glue-functions.
 * @param sourceMetadata            Associated source metadata for the metric
 * @param usageMetadata             Associated usage metadata for the metric
 * @param value                     The current metric value
 * @param meteredObjectCreationTime The creation time of the metered object. Optional, may be null.
 */
public record MetricValue(
    String id,
    String type,
    @Nullable Map<String, String> sourceMetadata,
    @Nullable Map<String, String> usageMetadata,
    long value,
    @Nullable Instant meteredObjectCreationTime
) {
    public MetricValue(String id, String type, Map<String, String> sourceMetadata, long value, Instant meteredObjectCreationTime) {
        this(id, type, sourceMetadata, null, value, meteredObjectCreationTime);
    }
}
