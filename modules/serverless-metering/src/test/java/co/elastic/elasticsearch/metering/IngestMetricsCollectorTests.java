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

import co.elastic.elasticsearch.metrics.MetricsCollector;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.BOOST_WINDOW_SETTING;
import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.SEARCH_POWER_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class IngestMetricsCollectorTests extends ESTestCase {
    protected final ClusterSettings clusterSettings = new ClusterSettings(
        Settings.builder().put(SEARCH_POWER_SETTING.getKey(), 100).put(BOOST_WINDOW_SETTING.getKey(), TimeValue.timeValueDays(5)).build(),
        Set.of(SEARCH_POWER_SETTING, BOOST_WINDOW_SETTING)
    );

    public void testMetricIdUniqueness() {
        var ingestMetricsCollector1 = new IngestMetricsCollector("node1", clusterSettings, Settings.EMPTY);
        var ingestMetricsCollector2 = new IngestMetricsCollector("node2", clusterSettings, Settings.EMPTY);

        ingestMetricsCollector1.addIngestedDocValue("index", 10);

        ingestMetricsCollector2.addIngestedDocValue("index", 20);

        MetricsCollector.MetricValue first = ingestMetricsCollector1.getMetrics().stream().findFirst().get();
        MetricsCollector.MetricValue second = ingestMetricsCollector2.getMetrics().stream().findFirst().get();

        assertThat(first.id(), equalTo("ingested-doc:index:node1"));
        assertThat(second.id(), equalTo("ingested-doc:index:node2"));
    }
}
