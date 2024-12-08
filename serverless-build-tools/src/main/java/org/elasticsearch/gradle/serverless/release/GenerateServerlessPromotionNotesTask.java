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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import org.apache.commons.lang3.tuple.Pair;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class GenerateServerlessPromotionNotesTask extends DefaultTask {

    private static final List<String> labelsToFilter = List.of(
        "auto-merge",
        "auto-merge-and-backport",
        "backport pending",
        "stateful-linked",
        "auto-merge-without-approval",
        "auto-backport",
        "backport",
        "v\\d.+"
    );

    private static final List<String> internalLabels = List.of(">non-issue", ">refactoring", ">test", ">test-failure", ">test-mute");

    @Input
    @Option(option = "currentGitHash", description = "The current promoting version")
    public abstract Property<String> getCurrentGitHash();

    @Input
    @Option(option = "previousGitHash", description = "The previous promoted version")
    public abstract Property<String> getPreviousGitHash();

    @Internal
    public abstract Property<String> getGithubToken();

    @Input
    public abstract Property<String> getHtmlReportName();

    @Input
    public abstract Property<String> getJsonReportName();

    @OutputDirectory
    public abstract DirectoryProperty getReportsDirectory();

    @TaskAction
    public void generatePromotionNotes() throws Exception {
        String currentGitHash = getCurrentGitHash().get();
        String previousGitHash = getPreviousGitHash().get();
        getLogger().lifecycle("Generating release notes for serverless releases");
        getLogger().lifecycle("Current git hash: " + currentGitHash);
        getLogger().lifecycle("Previous git hash: " + previousGitHash);

        GithubApi gh = new GithubApi("elastic", getGithubToken().get());
        List<PullRequest> allPullRequests = new ArrayList<>();
        List<PullRequest> serverlessPullRequests = gh.getPullRequestsFor("elasticsearch-serverless", previousGitHash, currentGitHash);
        getLogger().lifecycle("Found of " + serverlessPullRequests.size() + " pull requests for elasticsearch-serverless");
        allPullRequests.addAll(serverlessPullRequests);
        Pair<String, String> elasticsearchCommitRange = gh.resolveElasticsearchCommits(
            "elasticsearch-serverless",
            "elasticsearch",
            previousGitHash,
            currentGitHash
        );
        List<PullRequest> elasticsearchPullRequests = gh.getPullRequestsFor(
            "elasticsearch",
            elasticsearchCommitRange.getLeft(),
            elasticsearchCommitRange.getRight()
        );
        getLogger().lifecycle("Found " + elasticsearchPullRequests.size() + " pull requests for elasticsearch");
        allPullRequests.addAll(elasticsearchPullRequests);
        generateReports(allPullRequests);
    }

    private void generateReports(List<PullRequest> allPullRequests) throws IOException {
        // Get the current date in a readable format
        String currentDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMMM dd, yyyy HH:mm:ss"));
        Predicate<PullRequest> labelFilterPredicate = pr -> pr.getLabels()
            .stream()
            .map(l -> l.getName())
            .anyMatch(givenLabel -> internalLabels.stream().anyMatch(iLabel -> givenLabel.matches(iLabel)));
        List<PullRequest> serverlessPrs = allPullRequests.stream()
            .filter(pr -> pr.getRepository().equals("elasticsearch-serverless"))
            .filter(labelFilterPredicate.negate())
            .toList();
        List<PullRequest> esPrs = allPullRequests.stream()
            .filter(pr -> pr.getRepository().equals("elasticsearch"))
            .filter(labelFilterPredicate.negate())
            .toList();
        List<PullRequest> nonIssuePrs = allPullRequests.stream().filter(labelFilterPredicate).toList();
        getLogger().lifecycle("Found " + nonIssuePrs.size() + "(total " + allPullRequests.size() + ") of internal pull requests");
        generateJsonReport(currentDateTime, allPullRequests);
        generateHtmlReport(currentDateTime, serverlessPrs, esPrs, nonIssuePrs);
    }

    private void generateJsonReport(String currentDateTime, List<PullRequest> allPullRequests) {
        PromotionReport report = new PromotionReport(
            currentDateTime,
            getPreviousGitHash().get(),
            getCurrentGitHash().get(),
            allPullRequests
        );

        // Register the custom serializer
        Gson gson = new GsonBuilder().registerTypeAdapter(PromotionReport.class, new PromotionReportSerializer())
            .registerTypeAdapter(PullRequest.class, new PullRequestSerializer())
            .setPrettyPrinting()
            .create();

        File reportFile = getReportsDirectory().get().file(getJsonReportName().get()).getAsFile();
        try (FileWriter writer = new FileWriter(reportFile)) {
            gson.toJson(report, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void generateHtmlReport(
        String currentDateTime,
        List<PullRequest> serverlessPrs,
        List<PullRequest> esPrs,
        List<PullRequest> internalPrs
    ) throws IOException {
        String serverlessPrsPrTableHtml = formatPullRequestInfo(serverlessPrs);
        String esPrsPrTableHtml = formatPullRequestInfo(esPrs);
        String nonIssuePrsHtml = formatPullRequestInfo(internalPrs);

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
            "internalPrs",
            nonIssuePrsHtml,
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
        Path reportFile = getReportsDirectory().get().file(getHtmlReportName().get()).getAsFile().toPath();
        Files.write(reportFile, templateContent.getBytes());
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
                        .filter(label -> labelsToFilter.stream().anyMatch(filterLabel -> label.getName().matches(filterLabel)) == false)
                        .map(label -> "<div style=\"background-color:#" + label.getColor() + "\">" + label.getName() + "</div>")
                        .collect(Collectors.joining())
                    + "</td></tr>"
            )
            .collect(Collectors.joining("\n"));
    }

    private record PromotionReport(
        String currentDateTime,
        String previousGithash,
        String currentGithash,
        List<PullRequest> allPullRequests
    ) {}

    class PromotionReportSerializer implements JsonSerializer<PromotionReport> {
        @Override
        public JsonElement serialize(PromotionReport report, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();

            // Custom field names
            jsonObject.addProperty("Date", report.currentDateTime());
            jsonObject.addProperty("from", report.previousGithash());
            jsonObject.addProperty("to", report.currentGithash());
            jsonObject.add("pullRequests", context.serialize(report.allPullRequests()));
            return jsonObject;
        }
    }

    class PullRequestSerializer implements JsonSerializer<PullRequest> {
        @Override
        public JsonElement serialize(PullRequest pullRequest, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("title", pullRequest.getTitle());
            jsonObject.addProperty("mergedAt", pullRequest.getMergedAt());
            jsonObject.addProperty("repository", pullRequest.getRepository());
            jsonObject.addProperty("url", pullRequest.getUrl());
            JsonElement labels = new Gson().toJsonTree(
                pullRequest.getLabels()
                    .stream()
                    .filter(label -> labelsToFilter.stream().anyMatch(filterLabel -> label.getName().matches(filterLabel)) == false)
                    .map(Label::getName)
                    .toList()
            );
            jsonObject.add("labels", labels);
            return jsonObject;
        }
    }
}
