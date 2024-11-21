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

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class MeteringFeatures implements FeatureSpecification {
    public static final NodeFeature INDEX_INFO_SUPPORTED = new NodeFeature("index_size.supported");
    public static final NodeFeature SAMPLED_METRICS_METADATA = new NodeFeature("metering.sampled-metrics-metadata");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(INDEX_INFO_SUPPORTED, SAMPLED_METRICS_METADATA);
    }
}
