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

import co.elastic.elasticsearch.metering.MeteringFeatures;
import co.elastic.elasticsearch.metering.codec.RAStorageDocValuesFormatFactory;

module org.elasticsearch.metering {
    requires org.apache.logging.log4j;

    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.metrics;
    requires java.net.http;
    requires org.apache.lucene.core;
    requires org.elasticsearch.serverless.constants;
    requires org.elasticsearch.stateless.api;
    requires org.elasticsearch.xcore;

    provides org.elasticsearch.features.FeatureSpecification with MeteringFeatures;
    provides co.elastic.elasticsearch.stateless.api.DocValuesFormatFactory with RAStorageDocValuesFormatFactory;

    exports co.elastic.elasticsearch.metering.action to org.elasticsearch.server;
    exports co.elastic.elasticsearch.metering.codec to org.elasticsearch.server;
    exports co.elastic.elasticsearch.metering.sampling to org.elasticsearch.server;
    exports co.elastic.elasticsearch.metering.sampling.action to org.elasticsearch.server;
    exports co.elastic.elasticsearch.metering.usagereports.action to org.elasticsearch.server;
    exports co.elastic.elasticsearch.metering.activitytracking to org.elasticsearch.server;
}
