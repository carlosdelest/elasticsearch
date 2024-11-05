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

package org.elasticsearch.gradle.serverless.release;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.ProviderFactory;

import javax.inject.Inject;

public abstract class ServerlessPromotionReportPlugin implements Plugin<Project> {

    @Inject
    public abstract ProviderFactory getProviderFactory();

    @Inject
    public abstract ProjectLayout getProjectLayout();

    @Override
    public void apply(Project target) {
        target.getTasks().register("generatePromotionReport", GenerateServerlessPromotionNotesTask.class, task -> {
            task.setDescription("Generates promotion report for serverless distributions");
            task.setGroup("Serverless");
            task.getReportsDirectory().set(getProjectLayout().getBuildDirectory().dir("reports/promotion"));
            task.getHtmlReportName().set("serverless-promotion-report.html");
            task.getJsonReportName().set("serverless-promotion-report.json");
            task.getCurrentGitHash().convention(getProviderFactory().systemProperty("current.promoted.version"));
            task.getPreviousGitHash().convention(getProviderFactory().systemProperty("previous.promoted.version"));
            task.getGithubToken().set(getProviderFactory().environmentVariable("GITHUB_TOKEN"));
        });
    }
}
