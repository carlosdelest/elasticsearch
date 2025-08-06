/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.stats;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.index.bulk.stats.BulkStats;

import java.util.concurrent.TimeUnit;

public class RecallStats {

    private static final double ALPHA = 0.1;

    private final ExponentiallyWeightedMovingAverage recallEwma;

    public RecallStats() {
        this.recallEwma = new ExponentiallyWeightedMovingAverage(ALPHA, 0.0);
    }

}
