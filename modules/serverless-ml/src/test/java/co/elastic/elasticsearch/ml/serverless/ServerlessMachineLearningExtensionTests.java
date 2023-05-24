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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearningExtension;

public class ServerlessMachineLearningExtensionTests extends ESTestCase {

    public void testUseIlm() {
        MachineLearningExtension mlServerlessExtension = new ServerlessMachineLearningExtension();
        assertFalse(mlServerlessExtension.useIlm());
    }

    public void testIncludeNodeInfo() {
        MachineLearningExtension mlServerlessExtension = new ServerlessMachineLearningExtension();
        assertFalse(mlServerlessExtension.includeNodeInfo());
    }

    public void testIsAnomalyDetectionEnabled() {
        MachineLearningExtension mlServerlessExtension = new ServerlessMachineLearningExtension();
        expectThrows(IllegalStateException.class, mlServerlessExtension::isAnomalyDetectionEnabled);
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(Settings.EMPTY);
        assertTrue(mlServerlessExtension.isAnomalyDetectionEnabled());
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(
            Settings.builder().put(ServerlessMachineLearningPlugin.ANOMALY_DETECTION_ENABLED.getKey(), true).build()
        );
        assertTrue(mlServerlessExtension.isAnomalyDetectionEnabled());
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(
            Settings.builder().put(ServerlessMachineLearningPlugin.ANOMALY_DETECTION_ENABLED.getKey(), false).build()
        );
        assertFalse(mlServerlessExtension.isAnomalyDetectionEnabled());
    }

    public void testIsDataFrameAnalyticsEnabled() {
        MachineLearningExtension mlServerlessExtension = new ServerlessMachineLearningExtension();
        expectThrows(IllegalStateException.class, mlServerlessExtension::isDataFrameAnalyticsEnabled);
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(Settings.EMPTY);
        assertTrue(mlServerlessExtension.isDataFrameAnalyticsEnabled());
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(
            Settings.builder().put(ServerlessMachineLearningPlugin.DATA_FRAME_ANALYTICS_ENABLED.getKey(), true).build()
        );
        assertTrue(mlServerlessExtension.isDataFrameAnalyticsEnabled());
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(
            Settings.builder().put(ServerlessMachineLearningPlugin.DATA_FRAME_ANALYTICS_ENABLED.getKey(), false).build()
        );
        assertFalse(mlServerlessExtension.isDataFrameAnalyticsEnabled());
    }

    public void testIsNlpEnabled() {
        MachineLearningExtension mlServerlessExtension = new ServerlessMachineLearningExtension();
        expectThrows(IllegalStateException.class, mlServerlessExtension::isNlpEnabled);
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(Settings.EMPTY);
        assertTrue(mlServerlessExtension.isNlpEnabled());
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(Settings.builder().put(ServerlessMachineLearningPlugin.NLP_ENABLED.getKey(), true).build());
        assertTrue(mlServerlessExtension.isNlpEnabled());
        mlServerlessExtension = new ServerlessMachineLearningExtension();
        mlServerlessExtension.configure(Settings.builder().put(ServerlessMachineLearningPlugin.NLP_ENABLED.getKey(), false).build());
        assertFalse(mlServerlessExtension.isNlpEnabled());
    }
}
