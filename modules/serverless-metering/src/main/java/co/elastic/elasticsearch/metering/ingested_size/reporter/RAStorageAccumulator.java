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

package co.elastic.elasticsearch.metering.ingested_size.reporter;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.DocumentSizeAccumulator;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RAStorageAccumulator implements DocumentSizeAccumulator {
    private static final Logger logger = LogManager.getLogger(RAStorageAccumulator.class);
    public static final String RA_STORAGE_KEY = "_rastorage";
    public static final String RA_STORAGE_AVG_KEY = "_rastorage_avg";
    private final AtomicLong counter = new AtomicLong();

    @Override
    public void add(long size) {
        counter.addAndGet(size);
        logger.trace(() -> "accumulating size " + size + " total: " + counter);
    }

    @Override
    public Map<String, String> getAsCommitUserData(SegmentInfos segmentInfos) {
        Map<String, String> prevUserDataMap = segmentInfos.getUserData();
        long aggregatedSize = counter.getAndSet(0);
        String previousValue = prevUserDataMap.get(RA_STORAGE_KEY);
        long prevAggregatedSize = previousValue != null ? Long.parseLong(previousValue) : 0;

        long total = prevAggregatedSize + aggregatedSize;
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
