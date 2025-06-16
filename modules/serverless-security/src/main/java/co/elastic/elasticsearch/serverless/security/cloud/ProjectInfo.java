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

package co.elastic.elasticsearch.serverless.security.cloud;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

/**
 * Represents the project information in a serverless environment.
 */
public record ProjectInfo(String projectId, String organizationId, String projectType) {

    public ProjectInfo(String projectId, String organizationId, ProjectType projectType) {
        this(projectId, organizationId, toProjectTypeLiteral(projectType));
    }

    /**
     * Converts the main type of the {@link ProjectType} (which includes the subtypes).
     */
    private static String toProjectTypeLiteral(ProjectType projectType) {
        return switch (projectType) {
            case ProjectType.ELASTICSEARCH_GENERAL_PURPOSE, ProjectType.ELASTICSEARCH_SEARCH, ProjectType.ELASTICSEARCH_VECTOR,
                ProjectType.ELASTICSEARCH_TIMESERIES -> "elasticsearch";
            case ProjectType.OBSERVABILITY -> "observability";
            case ProjectType.SECURITY -> "security";
        };
    }

}
