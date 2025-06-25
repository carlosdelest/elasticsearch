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

package co.elastic.elasticsearch.serverless.observability;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;

import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.junit.ClassRule;

public class LogsEssentialsTierEsqlVerifiersIT extends AbstractEsqlVerifiersIT {

    @ClassRule
    public static ServerlessElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .setting("serverless.project_type", "observability")
        .setting("serverless.observability.tier", ObservabilityTier.LOGS_ESSENTIALS.name())
        .build();

    @Override
    protected ObservabilityTier getObservabilityTier() {
        return ObservabilityTier.LOGS_ESSENTIALS;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
