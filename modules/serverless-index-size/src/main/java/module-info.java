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

import co.elastic.elasticsearch.serverless.indexsize.IndexSizeFeatures;

module org.elasticsearch.serverless.indexsize {

    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.serverless.constants;

    provides org.elasticsearch.features.FeatureSpecification with IndexSizeFeatures;

    exports co.elastic.elasticsearch.serverless.indexsize;
    exports co.elastic.elasticsearch.serverless.indexsize.action to org.elasticsearch.server;
}
