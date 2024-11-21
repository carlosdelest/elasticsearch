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

package co.elastic.elasticsearch.metering.usagereports;

import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.util.Optional;

/**
 * A simple in-memory implementation of SampledMetricsTimeCursor. Not resilient to node restarts/MeteringIndexInfo PersistentTask migration
 * from one node to another.
 */
class InMemorySampledMetricsTimeCursor implements SampledMetricsTimeCursor {

    private volatile Instant latestCommittedTimestamp = null;

    InMemorySampledMetricsTimeCursor(Instant initialTime) {
        this.latestCommittedTimestamp = initialTime;
    }

    InMemorySampledMetricsTimeCursor() {
        this(Instant.now());
    }

    @Override
    public Optional<Instant> getLatestCommittedTimestamp() {
        return Optional.ofNullable(latestCommittedTimestamp);
    }

    @Override
    public Timestamps generateSampleTimestamps(Instant current, TimeValue decrement) {
        Instant timestamp = latestCommittedTimestamp;
        return timestamp != null
            ? SampledMetricsTimeCursor.generateSampleTimestamps(current, timestamp, decrement)
            : Timestamps.single(current);
    }

    @Override
    public boolean commitUpTo(Instant sampleTimestamp) {
        latestCommittedTimestamp = sampleTimestamp;
        return true;
    }
}
