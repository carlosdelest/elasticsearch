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
import co.elastic.elasticsearch.metering.activitytracking.ActivityTests;
import co.elastic.elasticsearch.metrics.MetricValue;
import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.metering.UsageMetadata.ACTIVE;
import static co.elastic.elasticsearch.metering.UsageMetadata.APPLICATION_TIER;
import static co.elastic.elasticsearch.metering.UsageMetadata.LATEST_ACTIVITY_TIMESTAMP;
import static co.elastic.elasticsearch.metering.UsageMetadata.SP_MIN;
import static co.elastic.elasticsearch.metering.UsageMetadata.SP_MIN_PROVISIONED_MEMORY;
import static co.elastic.elasticsearch.metering.UsageMetadata.SP_MIN_STORAGE_RAM_RATIO;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class VCUSampledMetricsBackfillStrategyTests extends ESTestCase {
    private static final Duration COOL_DOWN = Duration.ofMinutes(15);

    public static final Instant TIME = Instant.parse("2023-01-01T00:10:00Z");

    static class ValueConsumer implements SampledMetricsProvider.BackfillSink {
        AtomicReference<Long> value = new AtomicReference<>();
        AtomicReference<Map<String, String>> metadata = new AtomicReference<>();

        @Override
        public void add(MetricValue sample, long usageValue, Map<String, String> usageMetadata, Instant timestamp) {
            assertNull(this.value.getAndSet(usageValue));
            assertNull(this.metadata.getAndSet(usageMetadata));
        }
    }

    private static MetricValue metricValue(long value, Map<String, String> usageMetadata, Instant creationTime) {
        return new MetricValue("id", "type", null, usageMetadata, value, creationTime);
    }

    private static MetricValue metricValue(long value, Map<String, String> usageMetadata) {
        return metricValue(value, usageMetadata, null);
    }

    public void testConstantBackfillSinkCalledIfCreationEmpty() {
        var backfill = new VCUSampledMetricsBackfillStrategy(
            ActivityTests.randomActivity(),
            ActivityTests.randomActivity(),
            COOL_DOWN,
            LongCounter.NOOP
        );
        var usageMetadata = Map.of(APPLICATION_TIER, randomFrom("index", "search"));
        // accept if no creation date, vcu provider never sets a creation date
        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);
        assertNotNull(sink.value.get());

    }

    public void testConstantBackfillVCU() {
        var indexActivity = ActivityTests.randomActivity();
        var searchActivity = ActivityTests.randomActivity();
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN, LongCounter.NOOP);
        var usageMetadata = Map.of(APPLICATION_TIER, randomFrom("search", "index"));

        // accept if no creation date
        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);

        // vcu value
        assertThat(sink.value.get(), equalTo(value.value()));
    }

    public void testConstantBackfillSpMin() {
        var indexActivity = ActivityTests.randomActivity();
        var searchActivity = ActivityTests.randomActivity();
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN, LongCounter.NOOP);

        var spMinProvisionedMemory = String.valueOf(randomLongBetween(0, 10_000));
        var spMin = String.valueOf(randomLongBetween(0, 100));
        var spMinStorageRamRatio = Strings.format1Decimals(randomDouble() * 100, "");
        var usageMetadata = Map.of(
            APPLICATION_TIER,
            "search",
            SP_MIN_PROVISIONED_MEMORY,
            spMinProvisionedMemory,
            SP_MIN,
            spMin,
            SP_MIN_STORAGE_RAM_RATIO,
            spMinStorageRamRatio
        );

        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);

        assertThat(sink.metadata.get(), hasEntry(SP_MIN_PROVISIONED_MEMORY, spMinProvisionedMemory));
        assertThat(sink.metadata.get(), hasEntry(SP_MIN, spMin));
        assertThat(sink.metadata.get(), hasEntry(SP_MIN_STORAGE_RAM_RATIO, spMinStorageRamRatio));
    }

    public void testConstantBackfillMissingSpMin() {
        var indexActivity = ActivityTests.randomActivity();
        var searchActivity = ActivityTests.randomActivity();
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN, LongCounter.NOOP);

        var usageMetadata = Map.of(APPLICATION_TIER, "search");

        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);

        assertThat(sink.metadata.get(), not(hasKey(SP_MIN_PROVISIONED_MEMORY)));
        assertThat(sink.metadata.get(), not(hasKey(SP_MIN)));
    }

    public void testInterpolate() {
        var currentTime = randomBoolean() ? Instant.now() : Instant.ofEpochMilli(randomNonNegativeLong());
        var backfillTime = currentTime.minus(ActivityTests.randomDuration(Duration.ZERO, Duration.ofHours(1)));
        var previousTime = backfillTime.minus(ActivityTests.randomDuration(Duration.ZERO, Duration.ofHours(1)));
        var tier = randomFrom("index", "search");

        var spMin1 = randomLongBetween(0, 300);
        var spMin2 = randomLongBetween(0, 300);
        var spMinProvisioned1 = randomLongBetween(0, 10_000);
        var spMinProvisioned2 = randomLongBetween(0, 10_000);
        var vcu1 = randomLongBetween(0, 10_000);
        var vcu2 = randomLongBetween(0, 10_000);
        var ratio1 = randomDouble() * 100;
        var ratio2 = randomDouble() * 100;

        var metadata1 = Map.of(
            APPLICATION_TIER,
            tier,
            SP_MIN_PROVISIONED_MEMORY,
            Long.toString(spMinProvisioned1),
            SP_MIN,
            Long.toString(spMin1),
            SP_MIN_STORAGE_RAM_RATIO,
            Strings.format1Decimals(ratio1, "")
        );
        var metadata2 = Map.of(
            APPLICATION_TIER,
            tier,
            SP_MIN_PROVISIONED_MEMORY,
            Long.toString(spMinProvisioned2),
            SP_MIN,
            Long.toString(spMin2),
            SP_MIN_STORAGE_RAM_RATIO,
            Strings.format1Decimals(ratio2, "")
        );
        var sample1 = metricValue(vcu1, metadata1);
        var sample2 = metricValue(vcu2, metadata2);
        var backfill = new VCUSampledMetricsBackfillStrategy(
            ActivityTests.randomActivity(),
            ActivityTests.randomActivity(),
            COOL_DOWN,
            LongCounter.NOOP
        );

        var expectedVCU = Long.min(vcu1, vcu2);
        var expectedSpMinProvisioned = Long.min(spMinProvisioned1, spMinProvisioned2);
        var expectedSpMin = spMinProvisioned1 < spMinProvisioned2 ? spMin1 : spMin2;
        var expectedRatio = spMinProvisioned1 < spMinProvisioned2 ? ratio1 : ratio2;

        var sink = new ValueConsumer();
        backfill.interpolate(currentTime, sample1, previousTime, sample2, backfillTime, sink);
        assertThat(sink.value.get(), equalTo(expectedVCU));
        assertSpMinInfo(sink, expectedSpMinProvisioned, expectedSpMin, expectedRatio);

    }

    /**
     * Test constant and interpolate activity results together
     */
    public void testEmptyActivity() {
        boolean isSearch = randomBoolean();
        var searchActivity = isSearch ? Activity.EMPTY : ActivityTests.randomActivityNotEmpty();
        var indexActivity = isSearch ? ActivityTests.randomActivityNotEmpty() : Activity.EMPTY;
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN, LongCounter.NOOP);
        var tier = isSearch ? "search" : "index";

        var usageMetadata = Map.of(APPLICATION_TIER, tier);
        final var currentTime = randomBoolean() ? Instant.now() : Instant.ofEpochMilli(randomNonNegativeLong());
        final var previousTime = currentTime.minus(ActivityTests.randomDuration(Duration.ZERO, Duration.ofDays(1)));
        var current = metricValue(1L, usageMetadata);
        var previous = metricValue(1L, usageMetadata);

        var backfillTime = randomInstantBetween(previousTime, currentTime);

        {
            var sink = new ValueConsumer();
            backfill.constant(current, backfillTime, sink);
            assertDefault(sink, tier);
        }
        {
            var sink = new ValueConsumer();
            backfill.interpolate(currentTime, current, previousTime, previous, backfillTime, sink);
            assertDefault(sink, tier);
        }
    }

    /**
     * Test constant and interpolate activity results together
     */
    public void testNonEmptyActivity() {
        boolean isSearch = randomBoolean();
        var searchActivity = isSearch ? ActivityTests.randomActivityNotEmpty() : Activity.EMPTY;
        var indexActivity = isSearch ? Activity.EMPTY : ActivityTests.randomActivityNotEmpty();
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN, LongCounter.NOOP);
        var tier = isSearch ? "search" : "index";
        var activity = isSearch ? searchActivity : indexActivity;

        var lastActivity = activity.lastActivityRecentPeriod();
        var currentTime = randomInstantBetween(lastActivity, lastActivity.plus(COOL_DOWN.multipliedBy(2)));
        var previouslyActive = randomBoolean();
        var hasPreviousLastActivity = previouslyActive || randomBoolean(); // if not active may not have last activity time
        var previousLastActivity = randomInstantBetween(
            activity.firstActivity().minus(COOL_DOWN.multipliedBy(5)),
            activity.firstActivity().minus(COOL_DOWN)
        );
        var previousTime = randomInstantBetween(previousLastActivity, activity.firstActivity());
        var backfillTime = randomInstantBetween(previousTime, currentTime);

        var currentMetadata = buildMetadata(tier, true, lastActivity);
        var previousMetadata = buildMetadata(tier, previouslyActive, hasPreviousLastActivity ? previousLastActivity : null);
        var current = metricValue(1L, currentMetadata);
        var previous = metricValue(1L, previousMetadata);

        {
            var sink = new ValueConsumer();
            backfill.interpolate(currentTime, current, previousTime, previous, backfillTime, sink);
            var expected = activity.activitySnapshot(backfillTime, hasPreviousLastActivity ? previousLastActivity : null, COOL_DOWN);
            assertActiveInfo(sink, tier, expected.active(), expected.lastActivity());
        }
        {
            var sink = new ValueConsumer();
            backfill.constant(current, backfillTime, sink);
            var expected = activity.activitySnapshot(backfillTime, COOL_DOWN);
            assertActiveInfo(sink, tier, expected.active(), expected.lastActivity());
        }
    }

    private static void assertDefault(ValueConsumer sink, String tier) {
        assertActiveInfo(sink, tier, false, Instant.EPOCH);
    }

    private static void assertActiveInfo(ValueConsumer sink, String tier, boolean expectedActive, Instant expectedLastActivity) {
        assertThat(sink.metadata.get(), hasEntry(APPLICATION_TIER, tier));
        assertThat(sink.metadata.get(), hasEntry(ACTIVE, String.valueOf(expectedActive)));
        if (expectedLastActivity.equals(Instant.EPOCH)) {
            assertThat(sink.metadata.get(), not(hasKey(LATEST_ACTIVITY_TIMESTAMP)));
        } else {
            assertThat(sink.metadata.get(), hasEntry(LATEST_ACTIVITY_TIMESTAMP, expectedLastActivity.toString()));
        }
    }

    private static void assertSpMinInfo(ValueConsumer sink, long expectedSpMinProvisioned, long expectedSpMin, double expectedRatio) {
        assertThat(
            sink.metadata.get(),
            allOf(
                hasEntry(SP_MIN_PROVISIONED_MEMORY, (Long.toString(expectedSpMinProvisioned))),
                hasEntry(SP_MIN, (Long.toString(expectedSpMin))),
                hasEntry(SP_MIN_STORAGE_RAM_RATIO, (Strings.format1Decimals(expectedRatio, "")))
            )
        );
    }

    private static Map<String, String> buildMetadata(String tier, boolean active, @Nullable Instant lastActivity) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(APPLICATION_TIER, tier);
        metadata.put(ACTIVE, Boolean.toString(active));
        if (lastActivity != null) {
            metadata.put(LATEST_ACTIVITY_TIMESTAMP, lastActivity.toString());
        }
        return metadata;
    }
}
