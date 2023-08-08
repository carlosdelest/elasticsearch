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

package co.elastic.elasticsearch.ml.serverless;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ml.MachineLearningExtension;

import java.util.Map;
import java.util.Set;

public class ServerlessMachineLearningExtension implements MachineLearningExtension {

    public static final DiscoveryNode SERVERLESS_VIRTUAL_ML_NODE = new DiscoveryNode(
        "serverless",
        "serverless",
        "serverless",
        "serverless",
        NetworkAddress.format(TransportAddress.META_ADDRESS),
        new TransportAddress(TransportAddress.META_ADDRESS, 0),
        Map.of(),
        Set.of(DiscoveryNodeRole.ML_ROLE),
        null
    );

    private static final Logger logger = LogManager.getLogger(ServerlessMachineLearningExtension.class);

    private final SetOnce<Settings> settings = new SetOnce<>();

    @Override
    public void configure(Settings settings) {
        this.settings.set(settings);
    }

    @Override
    public boolean useIlm() {
        logger.debug("useIlm returning false");
        return false;
    }

    @Override
    public boolean includeNodeInfo() {
        logger.debug("includeNodeInfo returning false");
        return false;
    }

    @Override
    public boolean isAnomalyDetectionEnabled() {
        if (settings.get() == null) {
            throw new IllegalStateException("Too early to find out if anomaly detection is enabled");
        }
        boolean isAnomalyDetectionEnabled = ServerlessMachineLearningPlugin.ANOMALY_DETECTION_ENABLED.get(settings.get());
        logger.debug("isAnomalyDetectionEnabled returning [{}]", isAnomalyDetectionEnabled);
        return isAnomalyDetectionEnabled;
    }

    @Override
    public boolean isDataFrameAnalyticsEnabled() {
        if (settings.get() == null) {
            throw new IllegalStateException("Too early to find out if data frame analytics is enabled");
        }
        boolean isDataFrameAnalyticsEnabled = ServerlessMachineLearningPlugin.DATA_FRAME_ANALYTICS_ENABLED.get(settings.get());
        logger.debug("isDataFrameAnalyticsEnabled returning [{}]", isDataFrameAnalyticsEnabled);
        return isDataFrameAnalyticsEnabled;
    }

    @Override
    public boolean isNlpEnabled() {
        if (settings.get() == null) {
            throw new IllegalStateException("Too early to find out if NLP is enabled");
        }
        boolean isNlpEnabled = ServerlessMachineLearningPlugin.NLP_ENABLED.get(settings.get());
        logger.debug("isNlpEnabled returning [{}]", isNlpEnabled);
        return isNlpEnabled;
    }
}
