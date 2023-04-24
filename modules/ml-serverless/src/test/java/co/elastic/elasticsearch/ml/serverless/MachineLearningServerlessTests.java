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

import org.elasticsearch.test.ESTestCase;

public class MachineLearningServerlessTests extends ESTestCase {

    public void testUseIlm() {
        MachineLearningServerless mlServerless = new MachineLearningServerless();
        assertFalse(mlServerless.useIlm());
    }

    public void testIncludeNodeInfo() {
        MachineLearningServerless mlServerless = new MachineLearningServerless();
        assertFalse(mlServerless.includeNodeInfo());
    }

    public void testIsAnomalyDetectionEnabled() {
        MachineLearningServerless mlServerless = new MachineLearningServerless();
        assertTrue(mlServerless.isAnomalyDetectionEnabled());
        /* TODO
        mlServerless = new MachineLearningServerless(
            Settings.builder().put(MachineLearningServerless.ANOMALY_DETECTION_ENABLED.getKey(), true).build()
        );
        assertTrue(mlServerless.isAnomalyDetectionEnabled());
        mlServerless = new MachineLearningServerless(
            Settings.builder().put(MachineLearningServerless.ANOMALY_DETECTION_ENABLED.getKey(), false).build()
        );
        assertFalse(mlServerless.isAnomalyDetectionEnabled());
        */
    }

    public void testIsDataFrameAnalyticsEnabled() {
        MachineLearningServerless mlServerless = new MachineLearningServerless();
        assertTrue(mlServerless.isDataFrameAnalyticsEnabled());
        /* TODO
        mlServerless = new MachineLearningServerless(
            Settings.builder().put(MachineLearningServerless.DATA_FRAME_ANALYTICS_ENABLED.getKey(), true).build()
        );
        assertTrue(mlServerless.isDataFrameAnalyticsEnabled());
        mlServerless = new MachineLearningServerless(
            Settings.builder().put(MachineLearningServerless.DATA_FRAME_ANALYTICS_ENABLED.getKey(), false).build()
        );
        assertFalse(mlServerless.isDataFrameAnalyticsEnabled());
         */
    }

    public void testIsNlpEnabled() {
        MachineLearningServerless mlServerless = new MachineLearningServerless();
        assertTrue(mlServerless.isNlpEnabled());
        /* TODO
        mlServerless = new MachineLearningServerless(Settings.builder().put(MachineLearningServerless.NLP_ENABLED.getKey(), true).build());
        assertTrue(mlServerless.isNlpEnabled());
        mlServerless = new MachineLearningServerless(Settings.builder().put(MachineLearningServerless.NLP_ENABLED.getKey(), false).build());
        assertFalse(mlServerless.isNlpEnabled());
        */
    }
}
