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

import co.elastic.elasticsearch.metrics.SampledMetricsProvider;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static <T> List<T> iterableToList(Iterable<T> iter) {
        List<T> list = new ArrayList<T>();
        for (var x : iter) {
            list.add(x);
        }
        return list;
    }

    public static Matcher<SampledMetricsProvider.MetricValues> hasBackfillStrategy(
        Matcher<SampledMetricsProvider.BackfillStrategy> matcher
    ) {
        return new FeatureMatcher<>(matcher, "has backfill strategy", "backfillStrategy") {
            @Override
            protected SampledMetricsProvider.BackfillStrategy featureValueOf(SampledMetricsProvider.MetricValues metricValues) {
                return metricValues.backfillStrategy();
            }
        };
    }
}
