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

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_ACTIVE;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_APPLICATION_TIER;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_LATEST_ACTIVITY_TIME;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN;
import static co.elastic.elasticsearch.metering.sampling.SampledVCUMetricsProvider.USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY;
import static co.elastic.elasticsearch.metering.sampling.VCUSampledMetricsBackfillStrategy.inferActivity;
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
        var backfill = new VCUSampledMetricsBackfillStrategy(ActivityTests.randomActivity(), ActivityTests.randomActivity(), COOL_DOWN);
        var usageMetadata = Map.of(USAGE_METADATA_APPLICATION_TIER, randomFrom("index", "search"));
        // accept if no creation date, vcu provider never sets a creation date
        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);
        assertNotNull(sink.value.get());
    }

    public void testConstantBackfillVCU() {
        var indexActivity = ActivityTests.randomActivity();
        var searchActivity = ActivityTests.randomActivity();
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN);
        var usageMetadata = Map.of(USAGE_METADATA_APPLICATION_TIER, randomFrom("search", "index"));

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
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN);

        var spMinProvisionedMemory = String.valueOf(randomLongBetween(0, 10_000));
        var spMin = String.valueOf(randomLongBetween(0, 100));
        var usageMetadata = Map.of(
            USAGE_METADATA_APPLICATION_TIER,
            "search",
            USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY,
            spMinProvisionedMemory,
            USAGE_METADATA_SP_MIN,
            spMin
        );

        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);

        assertThat(sink.metadata.get(), hasEntry(USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY, spMinProvisionedMemory));
        assertThat(sink.metadata.get(), hasEntry(USAGE_METADATA_SP_MIN, spMin));
    }

    public void testConstantBackfillMissingSpMin() {
        var indexActivity = ActivityTests.randomActivity();
        var searchActivity = ActivityTests.randomActivity();
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN);

        var usageMetadata = Map.of(USAGE_METADATA_APPLICATION_TIER, "search");

        var value = metricValue(randomNonNegativeLong(), usageMetadata);
        var sink = new ValueConsumer();
        backfill.constant(value, TIME, sink);

        assertThat(sink.metadata.get(), not(hasKey(USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY)));
        assertThat(sink.metadata.get(), not(hasKey(USAGE_METADATA_SP_MIN)));
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

        var metadata1 = Map.of(
            USAGE_METADATA_APPLICATION_TIER,
            tier,
            USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY,
            Long.toString(spMinProvisioned1),
            USAGE_METADATA_SP_MIN,
            Long.toString(spMin1)
        );
        var metadata2 = Map.of(
            USAGE_METADATA_APPLICATION_TIER,
            tier,
            USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY,
            Long.toString(spMinProvisioned2),
            USAGE_METADATA_SP_MIN,
            Long.toString(spMin2)
        );
        var sample1 = metricValue(vcu1, metadata1);
        var sample2 = metricValue(vcu2, metadata2);
        var backfill = new VCUSampledMetricsBackfillStrategy(ActivityTests.randomActivity(), ActivityTests.randomActivity(), COOL_DOWN);

        var expectedVCU = Long.min(vcu1, vcu2);
        var expectedSpMinProvisioned = Long.min(spMinProvisioned1, spMinProvisioned2);
        var expectedSpMin = Long.min(spMin1, spMin2);
        {
            var sink = new ValueConsumer();
            backfill.interpolate(currentTime, sample1, previousTime, sample2, backfillTime, sink);
            assertThat(sink.value.get(), equalTo(expectedVCU));
            assertSpMinInfo(sink, expectedSpMinProvisioned, expectedSpMin);
        }
        {
            var sink = new ValueConsumer();
            backfill.interpolate(currentTime, sample2, previousTime, sample1, backfillTime, sink);
            assertThat(sink.value.get(), equalTo(expectedVCU));
            assertSpMinInfo(sink, expectedSpMinProvisioned, expectedSpMin);
        }
    }

    /**
     * Test constant and interpolate activity results together
     */
    public void testEmptyActivity() {
        boolean isSearch = randomBoolean();
        var searchActivity = isSearch ? Activity.EMPTY : ActivityTests.randomActivityNotEmpty();
        var indexActivity = isSearch ? ActivityTests.randomActivityNotEmpty() : Activity.EMPTY;
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN);
        var tier = isSearch ? "search" : "index";

        var usageMetadata = Map.of(USAGE_METADATA_APPLICATION_TIER, tier);
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
        var backfill = new VCUSampledMetricsBackfillStrategy(searchActivity, indexActivity, COOL_DOWN);
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
            var expectedActiveInfo = inferActivity(
                activity,
                hasPreviousLastActivity ? previousLastActivity : null,
                backfillTime,
                COOL_DOWN
            );
            assertActiveInfo(sink, tier, expectedActiveInfo.active(), expectedActiveInfo.lastActivityTime());
        }
        {
            var sink = new ValueConsumer();
            backfill.constant(current, backfillTime, sink);
            var expectedActiveInfo = inferActivity(activity, null, backfillTime, COOL_DOWN);
            assertActiveInfo(sink, tier, expectedActiveInfo.active(), expectedActiveInfo.lastActivityTime());
        }
    }

    public void testInferActivity() {
        {
            // Default case
            // If there is no activity return not active
            var backfillTime = Instant.now();
            var previousLastActivity = randomBoolean()
                ? null
                : backfillTime.minus(ActivityTests.randomDuration(Duration.ZERO, COOL_DOWN.multipliedBy(2)));
            assertThat(inferActivity(Activity.EMPTY, previousLastActivity, backfillTime, COOL_DOWN), equalTo(Activity.DEFAULT_NOT_ACTIVE));
        }

        {
            // Default case
            // backfill time not covered by activity, but after coolDown period of previousLastActivity, return not active
            var activity = ActivityTests.randomActivityNotEmpty();
            Instant firstActivity = activity.firstActivity();
            var backfillTime = firstActivity.minus(ActivityTests.randomDuration(Duration.ofMillis(1), COOL_DOWN.multipliedBy(2)));
            var previousLastTime = backfillTime.minus(ActivityTests.randomDuration(COOL_DOWN.plusMillis(1), COOL_DOWN.multipliedBy(2)));
            assertThat(inferActivity(activity, previousLastTime, backfillTime, COOL_DOWN), equalTo(Activity.DEFAULT_NOT_ACTIVE));
        }

        {
            // backfill time not covered by activity, but within coolDown period of previousLastActivity
            var activity = ActivityTests.randomActivityNotEmpty();
            Instant firstActivity = activity.firstActivity();
            var backfillTime = firstActivity.minus(ActivityTests.randomDuration(Duration.ofMillis(1), COOL_DOWN.multipliedBy(2)));
            var previousLastTime = backfillTime.minus(ActivityTests.randomDuration(Duration.ZERO, COOL_DOWN));
            assertThat(
                inferActivity(activity, previousLastTime, backfillTime, COOL_DOWN),
                equalTo(new Activity.ActiveInfo(true, previousLastTime))
            );
        }

        {
            // backfill time covered by activity or after activity, expect same results as `wasActive` function
            var activity = ActivityTests.randomActivityNotEmpty();
            Instant firstActivity = activity.firstActivity();
            Instant lastActivity = activity.lastActivityRecentPeriod();
            var backfillTime = randomInstantBetween(firstActivity, lastActivity.plus(COOL_DOWN.multipliedBy(2)));
            var expected = activity.wasActive(backfillTime, COOL_DOWN).get();
            assertThat(inferActivity(activity, null, backfillTime, COOL_DOWN), equalTo(expected));
        }
    }

    private static void assertDefault(ValueConsumer sink, String tier) {
        assertActiveInfo(sink, tier, false, Instant.EPOCH);
    }

    private static void assertActiveInfo(ValueConsumer sink, String tier, boolean expectedActive, Instant expectedLastActivity) {
        assertThat(sink.metadata.get(), hasEntry(USAGE_METADATA_APPLICATION_TIER, tier));
        assertThat(sink.metadata.get(), hasEntry(USAGE_METADATA_ACTIVE, String.valueOf(expectedActive)));
        if (expectedLastActivity.equals(Instant.EPOCH)) {
            assertThat(sink.metadata.get(), not(hasKey(USAGE_METADATA_LATEST_ACTIVITY_TIME)));
        } else {
            assertThat(sink.metadata.get(), hasEntry(USAGE_METADATA_LATEST_ACTIVITY_TIME, expectedLastActivity.toString()));
        }
    }

    private static void assertSpMinInfo(ValueConsumer sink, long expectedSpMinProvisioned, long expectedSpMin) {
        var spMinProvisioned = Long.parseLong(sink.metadata.get().get(USAGE_METADATA_SP_MIN_PROVISIONED_MEMORY));
        var spMin = Long.parseLong(sink.metadata.get().get(USAGE_METADATA_SP_MIN));
        assertThat(spMinProvisioned, equalTo(expectedSpMinProvisioned));
        assertThat(spMin, equalTo(expectedSpMin));
    }

    private static Map<String, String> buildMetadata(String tier, boolean active, @Nullable Instant lastActivity) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(USAGE_METADATA_APPLICATION_TIER, tier);
        metadata.put(USAGE_METADATA_ACTIVE, Boolean.toString(active));
        if (lastActivity != null) {
            metadata.put(USAGE_METADATA_LATEST_ACTIVITY_TIME, lastActivity.toString());
        }
        return metadata;
    }
}
