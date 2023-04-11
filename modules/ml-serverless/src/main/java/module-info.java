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

module org.elasticsearch.ml.serverless {
    requires org.apache.logging.log4j;

    requires org.elasticsearch.logging;
    requires org.elasticsearch.ml;
    requires org.elasticsearch.server;

    provides org.elasticsearch.xpack.ml.MachineLearningExtension with co.elastic.elasticsearch.ml.serverless.MachineLearningServerless;
}
