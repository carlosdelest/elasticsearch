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

module org.elasticsearch.serverless.apifiltering {
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.server;

    exports co.elastic.elasticsearch.api.filtering to org.elasticsearch.server;
    exports co.elastic.elasticsearch.api.validation to org.elasticsearch.server;

    provides org.elasticsearch.plugins.internal.RestExtension with co.elastic.elasticsearch.api.filtering.ApiFilterRestExtension;
}
