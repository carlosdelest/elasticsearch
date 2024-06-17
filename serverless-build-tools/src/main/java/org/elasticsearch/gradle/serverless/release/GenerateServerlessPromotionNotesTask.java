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

import org.apache.commons.lang3.tuple.Pair;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class GenerateServerlessPromotionNotesTask extends DefaultTask {

    private static final List<String> labelsToFilter = List.of("auto-merge", "auto-merge-and-backport", "backport pending");
    @Input
    @Option(option = "currentGitHash", description = "The current promoting version")
    public abstract Property<String> getCurrentGitHash();

    @Input
    @Option(option = "previousGitHash", description = "The previous promoted version")
    public abstract Property<String> getPreviousGitHash();

    @Internal
    public abstract Property<String> getGithubToken();

    @OutputFile
    public abstract RegularFileProperty getReleaseNotesFile();

    @TaskAction
    public void generatePromotionNotes() throws Exception {
        String currentGitHash = getCurrentGitHash().get();
        String previousGitHash = getPreviousGitHash().get();
        getLogger().lifecycle("Generating release notes for serverless releases");
        getLogger().lifecycle("Generating release notes for serverless releases");
        getLogger().lifecycle("Current git hash: " + currentGitHash);
        getLogger().lifecycle("Previous git hash: " + previousGitHash);

        GithubApi gh = new GithubApi("elastic", getGithubToken().get());
        List<PullRequest> allPullRequests = new ArrayList<>();
        allPullRequests.addAll(gh.getPullRequestsFor("elasticsearch-serverless", previousGitHash, currentGitHash));
        Pair<String, String> elasticsearchCommitRange = gh.resolveElasticsearchCommits(
            "elasticsearch-serverless",
            "elasticsearch",
            previousGitHash,
            currentGitHash
        );
        allPullRequests.addAll(
            gh.getPullRequestsFor("elasticsearch", elasticsearchCommitRange.getLeft(), elasticsearchCommitRange.getRight())
        );

        allPullRequests.forEach(pr -> {
            getLogger().lifecycle(pr.getMergedAt() + " [" + pr.getRepository() + "] " + pr.getTitle() + " -- " + pr.getUrl());
        });

        generateReport(allPullRequests);
    }

    private void generateReport(List<PullRequest> allPullRequests) throws IOException {
        // Get the current date in a readable format
        String currentDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMMM dd, yyyy HH:mm:ss"));
        List<PullRequest> serverlessPrs = allPullRequests.stream()
            .filter(pr -> pr.getRepository().equals("elasticsearch-serverless"))
            .toList();
        List<PullRequest> esPrs = allPullRequests.stream().filter(pr -> pr.getRepository().equals("elasticsearch")).toList();
        String serverlessPrsPrTableHtml = formatPullRequestInfo(serverlessPrs);
        String esPrsPrTableHtml = formatPullRequestInfo(esPrs);

        generateHtmlReport(currentDateTime, serverlessPrsPrTableHtml, esPrsPrTableHtml);
    }

    private void generateHtmlReport(String currentDateTime, String serverlessPrsPrTableHtml, String esPrsPrTableHtml) throws IOException {
        Map<String, String> reportData = Map.of(
            "timestamp",
            currentDateTime,
            "promotionHash",
            getCurrentGitHash().get(),
            "previousPromotionHash",
            getPreviousGitHash().get(),
            "esServerlessPullRequestTableRows",
            serverlessPrsPrTableHtml,
            "esPullRequestTableRows",
            esPrsPrTableHtml,
            "footer",
            "&copy; " + Year.now().getValue() + " Elastic."
        );

        InputStream templateInputStream = getClass().getClassLoader()
            .getResourceAsStream("org/elasticsearch/gradle/serverless/release/promotion-report.template");
        // Read the template content from the input stream
        String templateContent;
        try (InputStreamReader templateReader = new InputStreamReader(templateInputStream)) {
            templateContent = new BufferedReader(templateReader).lines().collect(Collectors.joining(System.lineSeparator()));
        }
        for (Map.Entry<String, String> entry : reportData.entrySet()) {
            templateContent = templateContent.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        Files.write(getReleaseNotesFile().get().getAsFile().toPath(), templateContent.getBytes());
    }

    private static @NotNull String formatPullRequestInfo(List<PullRequest> pullRequests) {
        return pullRequests.stream()
            .map(
                pr -> "<tr><td  class=\"col-1\">"
                    + pr.getMergedAt()
                    + "</td><td  class=\"col-2\">"
                    + pr.getTitle()
                    + "</td><td class=\"col-3\"><a href=\""
                    + pr.getUrl()
                    + "\">"
                    + pr.getUrl()
                    + "</a></td><td class=\"col-4\">"
                    + pr.getLabels()
                    .stream()
                    .map(label -> "<div style=\"background-color:#" + label.getColor() + "\">" + label.getName() + "</div>")
                    .collect(Collectors.joining())
                    + "</td></tr>"
            )
            .collect(Collectors.joining("\n"));
    }

    private static List<PullRequest.Label> filterDevLabels(List<PullRequest.Label> labels) {
        return labels.stream().filter(label -> labelsToFilter.contains(label.getName()) == false).toList();
    }
}
