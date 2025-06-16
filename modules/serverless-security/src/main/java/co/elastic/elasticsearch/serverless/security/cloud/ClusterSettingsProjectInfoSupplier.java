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
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.util.function.Supplier;

/**
 * Supplier that provides project information based on the cluster settings.
 */
public final class ClusterSettingsProjectInfoSupplier implements Supplier<ProjectInfo> {

    private final String projectId;
    private final ProjectType projectType;
    private final String organizationId;

    public ClusterSettingsProjectInfoSupplier(Settings settings) {
        validate(settings);
        this.projectId = ServerlessSharedSettings.PROJECT_ID.get(settings);
        this.projectType = ServerlessSharedSettings.PROJECT_TYPE.get(settings);
        this.organizationId = ServerlessSharedSettings.ORGANIZATION_ID.get(settings);
    }

    private static void validate(Settings settings) {
        if (false == ServerlessSharedSettings.PROJECT_ID.exists(settings)
            || false == ServerlessSharedSettings.ORGANIZATION_ID.exists(settings)
            || false == ServerlessSharedSettings.PROJECT_TYPE.exists(settings)) {
            throw new SettingsException("Project ID, Project Type, and Organization ID must be set in the settings");
        }
    }

    @Override
    public ProjectInfo get() {
        return new ProjectInfo(projectId, organizationId, projectType);
    }

}
