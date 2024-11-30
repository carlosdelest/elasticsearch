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

import org.elasticsearch.core.Booleans;
import org.elasticsearch.internal.CompletionsPostingsFormatExtension;

public class ServerlessCompletionsPostingsFormatExtension implements CompletionsPostingsFormatExtension {

    public ServerlessCompletionsPostingsFormatExtension() {}

    @Override
    public String getFormatName() {
        // This system property can be removed once the codec is deployed. It is included in the interim to allow for testing
        // in a QA environment.
        return Booleans.parseBoolean(System.getProperty("serverless.codec.configurable_completions_postings_enabled"), false)
            ? Elasticsearch900Lucene100CompletionPostingsFormat.NAME
            : null;
    }
}
