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

package co.elastic.elasticsearch.serverless.constants;

/**
 * The type of a project in serverless.
 *
 * Project type constants may have subtypes, separated by an underscore from the main project type.
 */
public enum ProjectType {
    ELASTICSEARCH_SEARCH,
    ELASTICSEARCH_VECTOR,
    ELASTICSEARCH_TIMESERIES,
    OBSERVABILITY,
    SECURITY
}
