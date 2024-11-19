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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.VerificationTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PromotionBlockersCheck extends DefaultTask implements VerificationTask {

    private static final String LABEL_NEEDS_RISK = "needs:risk";
    private static final String LABEL_BLOCKER = "blocker";
    private static final String LABEL_STATEFUL = "stateful";

    @Internal
    public abstract Property<String> getGithubToken();

    @Internal
    public abstract Property<Boolean> isCi();

    @Input
    public abstract Property<String> getJsonReportName();

    @Input
    public abstract Property<Integer> getIgnoredLastDays();

    @Input
    public abstract Property<Boolean> getFailOnUntriaged();

    @OutputDirectory
    public abstract DirectoryProperty getReportsDirectory();

    @TaskAction
    public void check() throws Exception {
        GithubApi gh = new GithubApi("elastic", getGithubToken().get());
        verifyIssues(List.of(checkForNeedsRisk(gh), checkForBlocker(gh)));
    }

    private void verifyIssues(List<CheckedIssues> issues) {
        writeReport(issues);
        addReportAsBuildAnnotation(issues);
        maybeFail(issues);
    }

    private void maybeFail(List<CheckedIssues> issues) {
        StringBuilder message = new StringBuilder();
        for (CheckedIssues check : issues) {
            if (check.blocking() && check.issues().size() > 0) {
                if (check.issues().size() > 0) {
                    message.append("There are ")
                        .append(check.issues().size())
                        .append(" open issues with " + check.reason() + " that need to be addressed before promoting the release: \n")
                        .append(check.issues().stream().map(issue -> issue.getUrl()).collect(Collectors.joining("\n")))
                        .append("\n\n");
                }
            }
        }
        if (message.isEmpty() == false) {
            throw new GradleException(message.toString());
        }
    }

    private void addReportAsBuildAnnotation(List<CheckedIssues> issues) {
        if (issues.isEmpty() == false && isCi().get()) {
            StringBuilder blockerAnnotation = new StringBuilder("### Promotion Issues\n\n");
            Map<Boolean, List<CheckedIssues>> byBlocking = issues.stream().collect(Collectors.groupingBy(i -> i.blocking()));
            byBlocking.forEach((blocking, checkIssues) -> {
                checkIssues.forEach(
                    checkedIssue -> blockerAnnotation.append(
                        "#### "
                            + checkedIssue.reason
                            + " (" + checkedIssue.issues().size() + " total) "
                            + (blocking ? " (blocking promotion) " : " (not blocking promotion) ") + "\n"
                            + checkedIssue.issues().stream().map(i -> "- " + i.getUrl()).collect(Collectors.joining("\n"))
                            + "\n\n"
                    )
                );
            });
            List<CheckedIssues> blocker = byBlocking.get(true);
            boolean isFailure = blocker != null && blocker.isEmpty() == false;
            try {
                Process process = new ProcessBuilder(
                    "buildkite-agent",
                    "annotate",
                    blockerAnnotation.toString(),
                    "--context",
                    "promotion-" + LABEL_BLOCKER,
                    "--style",
                    (getIgnoreFailures() || isFailure ? "warning" : "error")
                ).start();
                process.waitFor(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                getLogger().error("Failed to add build annotation", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void writeReport(List<CheckedIssues> issues) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        File reportFile = getReportsDirectory().get().file(getJsonReportName().get()).getAsFile();
        try (FileWriter writer = new FileWriter(reportFile)) {
            gson.toJson(issues, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private CheckedIssues checkForNeedsRisk(GithubApi gh) throws IOException, InterruptedException {
        List<Issue> needsRiskIssues = new ArrayList<>();
        Collection<Issue> esNeedsRiskIssues = gh.getIssues("elastic", "elasticsearch", LABEL_NEEDS_RISK);
        getLogger().lifecycle(
            "Found " + esNeedsRiskIssues.size() + "  issues in elastic/elasticsearch with label '" + LABEL_NEEDS_RISK + "'"
        );
        needsRiskIssues.addAll(esNeedsRiskIssues);
        Collection<Issue> esServerlessNeedsRiskIssues = gh.getIssues("elastic", "elasticsearch-serverless", LABEL_NEEDS_RISK);
        getLogger().lifecycle(
            "Found "
                + esServerlessNeedsRiskIssues.size()
                + "  issues in elastic/elasticsearch-serverless with label '"
                + LABEL_NEEDS_RISK
                + "'"
        );
        needsRiskIssues.addAll(esServerlessNeedsRiskIssues);
        Instant NOW = Instant.now();
        Integer daysIgnored = getIgnoredLastDays().get();
        return new CheckedIssues(
            needsRiskIssues.stream()
                .filter(issue -> issue.getCreatedAt().isBefore(NOW.minus(daysIgnored, ChronoUnit.DAYS)))
                .collect(Collectors.toList()),
            LABEL_NEEDS_RISK + " label older than " + daysIgnored + " days",
            getFailOnUntriaged().get()
        );
    }

    private CheckedIssues checkForBlocker(GithubApi gh) throws IOException, InterruptedException {
        return new CheckedIssues(
            Stream.concat(
                gh.getIssues("elastic", "elasticsearch", LABEL_BLOCKER)
                    .stream()
                    .filter(issue -> issue.getLabels().stream().noneMatch(label -> label.getName().equals(LABEL_STATEFUL))),
                gh.getIssues("elastic", "elasticsearch-serverless", LABEL_BLOCKER).stream()
            ).collect(Collectors.toList()),
            LABEL_BLOCKER + " label",
            getIgnoreFailures() == false
        );
    }

    record CheckedIssues(List<Issue> issues, String reason, boolean blocking) {}
}
