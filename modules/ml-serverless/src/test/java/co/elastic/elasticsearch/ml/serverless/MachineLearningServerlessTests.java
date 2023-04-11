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
}
