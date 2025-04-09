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

package co.elastic.elasticsearch.metering.sampling;

import co.elastic.elasticsearch.metering.activitytracking.Activity;
import co.elastic.elasticsearch.metering.sampling.SPMinProvisionedMemoryCalculator.SPMinInfo;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.metering.UsageMetadata.SEARCH_TIER_ACTIVE;
import static co.elastic.elasticsearch.metering.UsageMetadata.SEARCH_TIER_LATEST_ACTIVITY_TIMESTAMP;
import static co.elastic.elasticsearch.metering.sampling.IndexSizeMetricsProvider.IX_METRIC_TYPE;
import static java.time.Instant.EPOCH;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.common.util.Maps.copyMapWithAddedOrReplacedEntry;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexSizeMetricsBackfillStrategyTests extends ESTestCase {
    private static final Duration COOL_DOWN = Duration.ofMinutes(15);

    private static final Instant TIME = Instant.parse("2023-01-01T00:10:00Z");
    private static final Activity SEARCH_ACTIVITY = new Activity(TIME, TIME.truncatedTo(HOURS), EPOCH, EPOCH);

    private IndexSizeMetricsBackfillStrategy backfill = new IndexSizeMetricsBackfillStrategy(SEARCH_ACTIVITY, COOL_DOWN);

    static class Sink implements SampledMetricsProvider.BackfillSink {
        Long usageValue = null;
        Map<String, String> usageMetadata = null;

        @Override
        public void add(MetricValue sample, long usageValue, Map<String, String> usageMetadata, Instant timestamp) {
            this.usageValue = usageValue;
            this.usageMetadata = usageMetadata;
        }
    }

    private static MetricValue ixMetricValue(long value, Instant creationTime, Map<String, String> usageMetadata) {
        return new MetricValue("id", IX_METRIC_TYPE, null, usageMetadata, value, creationTime);
    }

    private static Map<String, String> ixUsageMetadata(long spMinProvisionedMemory, Instant timestamp) {
        var usageMetadata = new HashMap<String, String>();
        var snapshot = SEARCH_ACTIVITY.activitySnapshot(timestamp, null, COOL_DOWN);
        var spMinInfo = new SPMinInfo(spMinProvisionedMemory, 100, 10.0);
        spMinInfo.appendToUsageMetadata(usageMetadata);
        snapshot.appendToUsageMetadata(usageMetadata, SEARCH_TIER_ACTIVE, SEARCH_TIER_LATEST_ACTIVITY_TIMESTAMP);
        return Collections.unmodifiableMap(usageMetadata);
    }

    public void testConstantIndexSizeBackfill() {
        var creationDate = randomBoolean() ? null : TIME.minusMillis(randomNonNegativeInt());
        var usageMetadata = ixUsageMetadata(1024, TIME);
        var sink = new Sink();

        var value = ixMetricValue(randomNonNegativeLong(), creationDate, usageMetadata);
        backfill.constant(value, TIME, sink);
        assertThat(sink.usageValue, equalTo(value.value()));
        assertThat(sink.usageMetadata, equalTo(usageMetadata));

        // transition to inactive after cool down period
        backfill.constant(value, TIME.plusMillis(COOL_DOWN.toMillis() + 1), sink);
        assertThat(sink.usageValue, equalTo(value.value()));
        assertThat(sink.usageMetadata, equalTo(copyMapWithAddedOrReplacedEntry(usageMetadata, SEARCH_TIER_ACTIVE, "false")));
    }

    public void testInterpolateIndexSize() {
        var provMemoryPrev = randomIntBetween(1, 10) * 1024;
        var provMemoryNow = randomIntBetween(1, 10) * 1024;

        var sink = new Sink();

        var samplePrev = ixMetricValue(0, null, ixUsageMetadata(provMemoryPrev, TIME));
        var sampleNow = ixMetricValue(300, null, ixUsageMetadata(provMemoryNow, TIME));
        var expectedUsageMetadata = (provMemoryPrev < provMemoryNow ? samplePrev : sampleNow).usageMetadata();
        backfill.interpolate(TIME.plusSeconds(300), sampleNow, TIME, samplePrev, TIME.plusSeconds(200), sink);

        assertThat(sink.usageValue, equalTo(200L));
        assertThat(sink.usageMetadata, equalTo(expectedUsageMetadata));

        backfill.interpolate(
            TIME.plusMillis(COOL_DOWN.toMillis() + 2),
            sampleNow,
            TIME,
            samplePrev,
            TIME.plusMillis(COOL_DOWN.toMillis() + 1),
            sink
        );

        assertThat(sink.usageValue, equalTo(300L));
        assertThat(sink.usageMetadata, equalTo(copyMapWithAddedOrReplacedEntry(expectedUsageMetadata, SEARCH_TIER_ACTIVE, "false")));

    }

    public void testNoConstantBackfillIfObjectNewer() {
        var value = ixMetricValue(randomNonNegativeLong(), TIME.plusMillis(1 + randomNonNegativeInt()), null);
        var sink = new Sink();
        // expect nothing to be emitted to the sink
        backfill.constant(value, TIME, sink);
        assertThat(sink.usageValue, nullValue());
    }
}
