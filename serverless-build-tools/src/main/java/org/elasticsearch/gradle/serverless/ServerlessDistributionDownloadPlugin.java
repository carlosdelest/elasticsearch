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

package org.elasticsearch.gradle.serverless;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.DistributionDownloadPlugin;
import org.elasticsearch.gradle.DistributionResolution;
import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.distribution.ElasticsearchDistributionTypes;
import org.elasticsearch.gradle.internal.InternalDistributionDownloadPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Extension of {@link InternalDistributionDownloadPlugin} which registers a {@link DistributionResolution} for resolving
 * serverless distributions for backward-compatible upgrade testing.
 *
 * Currently, this implementation relies on a convention of a version string of "0.0.0" indicating a serverless BWC build.
 * The intention is to do some upstream refactoring to remove the need for versions for distribution resolution.
 */
public class ServerlessDistributionDownloadPlugin implements Plugin<Project> {
    public static final Version SERVERLESS_BWC_VERSION = Version.fromString("0.0.0");

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(InternalDistributionDownloadPlugin.class);

        Collection<DistributionResolution> resolutions = DistributionDownloadPlugin.getRegistrationsContainer(project);
        registerDistributionResolver(resolutions);
    }

    private static void registerDistributionResolver(Collection<DistributionResolution> resolutions) {
        DistributionResolution serverlessBwcResolution = new DistributionResolution("serverless_bwc");
        serverlessBwcResolution.setPriority(100);
        serverlessBwcResolution.setResolver((project, distribution) -> {
            if (SERVERLESS_BWC_VERSION.equals(Version.fromString(distribution.getVersion()))) {
                return new InternalDistributionDownloadPlugin.ProjectBasedDistributionDependency(
                    config -> projectDependency(project, ":distribution:bwc", projectConfigurationName(distribution))
                );
            }
            return null;
        });
        resolutions.add(serverlessBwcResolution);
    }

    private static Dependency projectDependency(Project project, String projectPath, String projectConfig) {
        if (project.findProject(projectPath) == null) {
            throw new GradleException("no project [" + projectPath + "], project names: " + project.getRootProject().getAllprojects());
        }
        Map<String, Object> depConfig = new HashMap<>();
        depConfig.put("path", projectPath);
        depConfig.put("configuration", projectConfig);
        return project.getDependencies().project(depConfig);
    }

    private static String projectConfigurationName(ElasticsearchDistribution distribution) {
        if (distribution.getType() == ElasticsearchDistributionTypes.ARCHIVE) {
            String archString = distribution.getPlatform() == ElasticsearchDistribution.Platform.WINDOWS
                || distribution.getArchitecture() == Architecture.X64 ? "" : "-" + distribution.getArchitecture().toString().toLowerCase();
            String extension = distribution.getPlatform() == ElasticsearchDistribution.Platform.WINDOWS ? "zip" : "tar";

            return "bwc_" + distribution.getPlatform().toString() + archString + "-" + extension;
        } else {
            throw new IllegalArgumentException(
                "Distribution type [" + distribution.getType() + "] unsupported. Only archive serverless BWC distributions are supported."
            );
        }
    }
}
