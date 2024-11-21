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

package co.elastic.elasticsearch.metering.reporter;

import co.elastic.elasticsearch.metering.RaStorageMetadataFieldMapper;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RAStorageAccumulator implements DocumentSizeAccumulator {
    private static final Logger logger = LogManager.getLogger(RAStorageAccumulator.class);
    public static final String RA_STORAGE_KEY = RaStorageMetadataFieldMapper.FIELD_NAME;
    public static final String RA_STORAGE_AVG_KEY = "_rastorage_avg";

    private final AtomicLong counter = new AtomicLong();

    @Override
    public void add(long size) {
        assert size >= 0 : "size increment must be non-negative";
        counter.addAndGet(size);
        logger.trace(() -> "accumulating size " + size + " total: " + counter);
    }

    @Override
    public Map<String, String> getAsCommitUserData(SegmentInfos segmentInfos) {
        Map<String, String> prevUserDataMap = segmentInfos.getUserData();
        long aggregatedSize = counter.getAndSet(0);
        String previousValue = prevUserDataMap.get(RA_STORAGE_KEY);
        if (aggregatedSize == 0 && previousValue == null) {
            return Collections.emptyMap();
        }

        long prevAggregatedSize = previousValue != null ? Long.parseLong(previousValue) : 0;

        // Due to a bug related to ES-8577, we recorded the default raw size (-1, meaning not metered) for documents
        // replayed from translog. Ignore any negative previous RA-S total here.
        long total = Math.max(0, prevAggregatedSize) + aggregatedSize;
        logger.trace(
            "CommitUserData new size [{}] (previous [{}], new [{}]), generation [{}]",
            total,
            previousValue,
            aggregatedSize,
            segmentInfos.getGeneration()
        );
        return Map.of(RA_STORAGE_KEY, String.valueOf(total));
    }
}
