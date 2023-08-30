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

package co.elastic.elasticsearch.serverless.transform;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.TransformExtension;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServerlessTransformExtensionTests extends ESTestCase {

    private TransformExtension transformExtension = new ServerlessTransformExtension();

    public void testIncludeNodeInfo() {
        assertFalse(transformExtension.includeNodeInfo());
    }

    public void testGetTransformInternalIndexAdditionalSettings() {
        assertThat(
            transformExtension.getTransformInternalIndexAdditionalSettings(),
            is(equalTo(Settings.builder().put("index.fast_refresh", "true").build()))
        );
    }

    public void testGetTransformDestinationIndexSettings() {
        assertThat(transformExtension.getTransformDestinationIndexSettings(), is(equalTo(Settings.EMPTY)));
    }
}
