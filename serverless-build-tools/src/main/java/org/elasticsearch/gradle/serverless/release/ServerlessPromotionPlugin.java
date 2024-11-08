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

public abstract class ServerlessPromotionPlugin implements Plugin<Project> {

    private static final String TASK_GROUP = "Promotion";
    private static final String IGNORE_BLOCKER_ENV = "IGNORE_BLOCKER";
    private static final String GITHUB_TOKEN_ENV = "GITHUB_TOKEN";
    private static final String CHECK_PROMOTION_BLOCKER_TASKNAME = "checkPromotionBlocker";
    private static final String GENERATE_PROMOTION_REPORT_TASKNAME = "generatePromotionReport";
    private static final String CURRENT_PROMOTED_VERSION_SYSPROP = "current.promoted.version";
    private static final String PREVIOUS_PROMOTED_VERSION_SYSPROP = "previous.promoted.version";
    private static final String DAYS_BLOCKER_IGNORED_DAYS_ENV = "IGNORED_BLOCKER_DAYS";
    private static final String BLOCK_ON_ISSUES_UNTRIAGED = "BLOCK_ON_ISSUES_UNTRIAGED";

    private static final int DAYS_BLOCKER_IGNORED = 7;

    @Inject
    public abstract ProviderFactory getProviderFactory();

    @Inject
    public abstract ProjectLayout getProjectLayout();

    @Override
    public void apply(Project target) {
        target.getTasks().register(CHECK_PROMOTION_BLOCKER_TASKNAME, PromotionBlockersCheck.class, task -> {
            task.setDescription("Checks for blocker serverless distributions");
            task.setGroup(TASK_GROUP);
            task.getIgnoredLastDays()
                .set(
                    getProviderFactory().environmentVariable(DAYS_BLOCKER_IGNORED_DAYS_ENV)
                        .map(Integer::parseInt)
                        .orElse(DAYS_BLOCKER_IGNORED)
                );
            task.getGithubToken().set(getProviderFactory().environmentVariable(GITHUB_TOKEN_ENV));
            task.getReportsDirectory().set(getProjectLayout().getBuildDirectory().dir("reports/blockers"));
            task.getJsonReportName().set("serverless-promotion-blocker.json");
            task.getFailOnUntriaged()
                .set(getProviderFactory().environmentVariable(BLOCK_ON_ISSUES_UNTRIAGED).map(Boolean::parseBoolean).orElse(true).get());
            task.setIgnoreFailures(
                getProviderFactory().environmentVariable(IGNORE_BLOCKER_ENV).map(Boolean::parseBoolean).orElse(false).get()
            );
        });
        target.getTasks().register(GENERATE_PROMOTION_REPORT_TASKNAME, GenerateServerlessPromotionNotesTask.class, task -> {
            task.setDescription("Generates promotion report for serverless distributions");
            task.setGroup(TASK_GROUP);
            task.getReportsDirectory().set(getProjectLayout().getBuildDirectory().dir("reports/promotion"));
            task.getHtmlReportName().set("serverless-promotion-report.html");
            task.getJsonReportName().set("serverless-promotion-report.json");
            task.getCurrentGitHash().convention(getProviderFactory().systemProperty(CURRENT_PROMOTED_VERSION_SYSPROP));
            task.getPreviousGitHash().convention(getProviderFactory().systemProperty(PREVIOUS_PROMOTED_VERSION_SYSPROP));
            task.getGithubToken().set(getProviderFactory().environmentVariable(GITHUB_TOKEN_ENV));
        });
    }
}
