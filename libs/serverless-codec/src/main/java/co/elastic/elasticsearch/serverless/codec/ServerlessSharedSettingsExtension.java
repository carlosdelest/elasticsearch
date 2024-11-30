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

package co.elastic.elasticsearch.serverless.codec;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.internal.SettingsExtension;

import java.util.List;

public class ServerlessSharedSettingsExtension implements SettingsExtension {
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(Elasticsearch900Lucene100CompletionPostingsFormat.COMPLETION_FST_ON_HEAP);
    }
}
