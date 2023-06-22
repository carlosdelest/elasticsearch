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

package co.elastic.elasticsearch.serverless.autoscaling.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

public class TransportGetAutoScalingMetricsTests extends ESTestCase {

    public void testTierUniqueNaming() {
        assertEquals(
            TransportGetAutoscalingMetricsAction.TIER_ACTIONS.size(),
            TransportGetAutoscalingMetricsAction.TIER_ACTIONS.stream()
                .map(t -> t.v2().apply(TimeValue.ZERO).getTierName())
                .distinct()
                .count()
        );
    }

}
