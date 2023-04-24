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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.ml.MachineLearningExtension;

import java.util.List;

public class MachineLearningServerless extends Plugin implements MachineLearningExtension {

    private static final Logger logger = LogManager.getLogger(MachineLearningServerless.class);

    public static final String NAME = "ml-serverless";

    // These 3 settings enable parts of ML to be enabled or disabled at a more granular level than the entire plugin
    public static final Setting<Boolean> ANOMALY_DETECTION_ENABLED = Setting.boolSetting("xpack.ml.ad.enabled", true, Property.NodeScope);
    public static final Setting<Boolean> DATA_FRAME_ANALYTICS_ENABLED = Setting.boolSetting(
        "xpack.ml.dfa.enabled",
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> NLP_ENABLED = Setting.boolSetting("xpack.ml.nlp.enabled", true, Property.NodeScope);

    public MachineLearningServerless() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ANOMALY_DETECTION_ENABLED, DATA_FRAME_ANALYTICS_ENABLED, NLP_ENABLED);
    }

    @Override
    public boolean useIlm() {
        logger.debug("{}::useIlm returning false.", NAME);
        return false;
    }

    @Override
    public boolean includeNodeInfo() {
        logger.debug("{}::includeNodeInfo returning false.", NAME);
        return false;
    }

    @Override
    public boolean isAnomalyDetectionEnabled() {
        return true; // TODO ANOMALY_DETECTION_ENABLED.get(settings);
    }

    @Override
    public boolean isDataFrameAnalyticsEnabled() {
        return true; // TODO DATA_FRAME_ANALYTICS_ENABLED.get(settings);
    }

    @Override
    public boolean isNlpEnabled() {
        return true; // TODO NLP_ENABLED.get(settings);
    }
}
