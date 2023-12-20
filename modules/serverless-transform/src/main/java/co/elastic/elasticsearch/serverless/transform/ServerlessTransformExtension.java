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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.transform.TransformExtension;

public class ServerlessTransformExtension implements TransformExtension {

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(5);

    @Override
    public boolean includeNodeInfo() {
        return false;
    }

    @Override
    public Settings getTransformInternalIndexAdditionalSettings() {
        return Settings.builder().put(IndexSettings.INDEX_FAST_REFRESH_SETTING.getKey(), true).build();
    }

    @Override
    public Settings getTransformDestinationIndexSettings() {
        return Settings.EMPTY;
    }

    @Override
    public TimeValue getMinFrequency() {
        return MIN_FREQUENCY;
    }
}
