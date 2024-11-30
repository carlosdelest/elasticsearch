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

import co.elastic.elasticsearch.serverless.codec.CodecModuleQualifiedExportsService;
import co.elastic.elasticsearch.serverless.codec.Elasticsearch900Lucene100CompletionPostingsFormat;
import co.elastic.elasticsearch.serverless.codec.ServerlessCompletionsPostingsFormatExtension;

import org.elasticsearch.internal.CompletionsPostingsFormatExtension;
import org.elasticsearch.jdk.ModuleQualifiedExportsService;

module org.elasticsearch.serverless.codec {
    requires org.apache.logging.log4j;
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires org.apache.lucene.core;
    requires org.apache.lucene.suggest;

    exports co.elastic.elasticsearch.serverless.codec to org.elasticsearch.server, org.elasticsearch.stateless;

    provides org.elasticsearch.plugins.internal.SettingsExtension
        with
            co.elastic.elasticsearch.serverless.codec.ServerlessSharedSettingsExtension;
    provides CompletionsPostingsFormatExtension with ServerlessCompletionsPostingsFormatExtension;
    provides org.apache.lucene.codecs.PostingsFormat with Elasticsearch900Lucene100CompletionPostingsFormat;
    provides ModuleQualifiedExportsService with CodecModuleQualifiedExportsService;
}
