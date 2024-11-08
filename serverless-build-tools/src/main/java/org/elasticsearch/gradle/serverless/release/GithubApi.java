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
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GithubApi {
    private static final String GITHUB_PULL_REQUEST_COMMIT_PATTERN = ".+\\(#(\\d+)\\)";
    private static final Type LIST_PR_TYPE = new TypeToken<List<PullRequest>>() {
    }.getType();
    private static final Type PR_TYPE = new TypeToken<PullRequest>() {
    }.getType();
    private static final String APPLICATION_VND_GITHUB_JSON = "application/vnd.github+json";
    private static final String NEXT_PAGE_REGEX = ".*<(.+)>; rel=\"next\".*";
    private static final String GITHUB_API_BASE = "https://api.github.com/repos/";
    private static final String DEFAULT_PAGE_SIZE = "100";

    private final String owner;
    private final String token;
    private final Logger logger = Logging.getLogger(getClass());

    public GithubApi(String owner, String token) {
        this.owner = owner;
        this.token = token;
    }

    public List<PullRequest> getPullRequestsFor(String repositoryName, String commit1, String commit2) throws Exception {
        List<Commit> commitsBetween = getCommitsBetween(repositoryName, commit1, commit2).stream().toList();
        logger.lifecycle("Found {} commits in {} between {} and {}", commitsBetween.size(), repositoryName, commit1, commit2);
        List<Commit> revisitCommits = Lists.newArrayList();
        List<PullRequest> prs = new ArrayList<>();
        for (Commit commit : commitsBetween) {
            try {
                List<PullRequest> pullRequestsForCommit = getPullRequestsForCommit(owner, repositoryName, commit.getSha());
                if (pullRequestsForCommit.isEmpty()) {
                    revisitCommits.add(commit);
                } else {
                    prs.addAll(pullRequestsForCommit);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Pattern revisitPattern = Pattern.compile(GITHUB_PULL_REQUEST_COMMIT_PATTERN);
        for (Commit revisitCommit : revisitCommits) {
            Matcher matcher = revisitPattern.matcher(revisitCommit.getShortMessage());
            if (matcher.matches()) {
                String prNumber = matcher.group(1);
                PullRequest pr = getPullRequest(repositoryName, Integer.parseInt(prNumber));
                prs.add(pr);
            }
        }
        return prs;
    }

    private PullRequest getPullRequest(String repository, int pullRequestNumber) throws Exception {
        String apiUrl = GITHUB_API_BASE + owner + "/" + repository + "/pulls/" + pullRequestNumber;
        String response = getGitHubData(apiUrl);
        return new GsonBuilder().registerTypeAdapter(PullRequest.class, new PullRequestDeserializer()).create().fromJson(response, PR_TYPE);
    }

    public Pair<String, String> resolveElasticsearchCommits(String repositoryName, String submodulePath, String commit1, String commit2)
        throws Exception {
        String submoduleSha1 = getSubmoduleSha(repositoryName, submodulePath, commit1);
        String submoduleSha2 = getSubmoduleSha(repositoryName, submodulePath, commit2);
        return Pair.of(submoduleSha1, submoduleSha2);
    }

    private String getSubmoduleSha(String repositoryName, String submodulePath, String commit1) throws Exception {
        String treeData = getGitHubData(GITHUB_API_BASE + "elastic/" + repositoryName + "/git/trees/" + commit1);
        JsonObject treeJson = new Gson().fromJson(treeData, JsonObject.class);
        JsonArray treeArray = treeJson.getAsJsonArray("tree");
        String submoduleSha1 = findSubmoduleSha(treeArray, submodulePath);
        return submoduleSha1;
    }

    private List<PullRequest> getPullRequestsForCommit(String owner, String repo, String commitSha) throws IOException,
        InterruptedException {
        String url = String.format("https://api.github.com/repos/%s/%s/commits/%s/pulls", owner, repo, commitSha);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + token)
            .header("Accept", APPLICATION_VND_GITHUB_JSON)
            .GET()
            .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            return new GsonBuilder().registerTypeAdapter(PullRequest.class, new PullRequestDeserializer())
                .create()
                .fromJson(response.body(), LIST_PR_TYPE);
        } else {
            throw new IOException("Failed to retrieve PRs: " + response.statusCode() + " " + response.body());
        }
    }

    private static String findSubmoduleSha(JsonArray treeArray, String submodulePath) {
        for (JsonElement element : treeArray) {
            JsonObject entry = element.getAsJsonObject();
            if (entry.get("path").getAsString().equals(submodulePath) && entry.get("type").getAsString().equals("commit")) {
                return entry.get("sha").getAsString();
            }
        }
        return null;
    }

    private List<Commit> getCommitsBetween(String repo, String startSha, String endSha) throws Exception {
        String apiUrl = GITHUB_API_BASE + owner + "/" + repo + "/compare/" + startSha + "..." + endSha;
        String response = getGitHubData(apiUrl);
        com.google.gson.JsonElement jsonElement = new Gson().fromJson(response, com.google.gson.JsonElement.class)
            .getAsJsonObject()
            .get("commits");
        Type commitListType = new TypeToken<List<Commit>>() {
        }.getType();
        return new GsonBuilder().registerTypeAdapter(Commit.class, new CommitDeserializer()).create().fromJson(jsonElement, commitListType);
    }

    private String getGitHubData(String apiUrl) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(apiUrl))
            .header("Authorization", "token " + token)
            .header("Accept", APPLICATION_VND_GITHUB_JSON)
            .GET()
            .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            return response.body();
        } else {
            throw new IOException("Failed : HTTP Request: '" + apiUrl + " - Error code: " + response.statusCode());
        }
    }

    public Collection<Issue> getIssues(String owner, String repository, String label) throws IOException, InterruptedException {
        List<Issue> allIssues = new ArrayList<>();
        boolean morePages;
        String apiUrl = String.format(
            "https://api.github.com/repos/%s/%s/issues?labels=%s&state=open&per_page=" + DEFAULT_PAGE_SIZE,
            owner,
            repository,
            label
        );

        do {
            HttpResponse<String> response = requestIssuePage(apiUrl);
            if (response.statusCode() == 200) {
                Type issueType = new TypeToken<List<Issue>>() {
                }.getType();
                List<Issue> pagedIssues = new GsonBuilder().registerTypeAdapter(Issue.class, new IssueDeserializer())
                    .create()
                    .fromJson(response.body(), issueType);
                allIssues.addAll(pagedIssues);
            } else {
                throw new IOException("Failed to retrieve blocked issues: " + response.statusCode() + " " + response.body());
            }
            if (response.headers().firstValue("link").isPresent()) {
                String linkHeader = response.headers().firstValue("link").get();
                morePages = linkHeader.contains("rel=\"next\"");
                System.out.println("linkHeader = " + linkHeader + " morePages = " + morePages);
                if (morePages) {
                    Matcher matcher = Pattern.compile(NEXT_PAGE_REGEX).matcher(linkHeader);
                    if (matcher.matches()) {
                        apiUrl = matcher.group(1);
                    }
                }
            } else {
                morePages = false;
            }
        } while (morePages);
        return allIssues;
    }

    private HttpResponse<String> requestIssuePage(String apiUrl) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(apiUrl))
            .header("Authorization", "Bearer " + token)
            .header("Accept", APPLICATION_VND_GITHUB_JSON)
            .GET()
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response;
    }

    private static class CommitDeserializer implements JsonDeserializer<Commit> {
        @Override
        public Commit deserialize(com.google.gson.JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
            return new Commit(
                json.getAsJsonObject().get("sha").getAsString(),
                json.getAsJsonObject().get("commit").getAsJsonObject().get("author").getAsJsonObject().get("date").getAsString(),
                json.getAsJsonObject().get("commit").getAsJsonObject().get("message").getAsString()
            );
        }
    }

    private static class IssueDeserializer implements JsonDeserializer<Issue> {
        // Define the regex pattern to extract the repository name

        @Override
        public Issue deserialize(com.google.gson.JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
            Type labelsListType = new TypeToken<List<Label>>() {
            }.getType();
            List<Label> labels = new Gson().fromJson(json.getAsJsonObject().get("labels").getAsJsonArray(), labelsListType);

            return new Issue(
                json.getAsJsonObject().get("title").getAsString(),
                json.getAsJsonObject().get("html_url").getAsString(),
                json.getAsJsonObject().get("created_at").getAsString(),
                labels
            );

        }
    }

    private static class PullRequestDeserializer implements JsonDeserializer<PullRequest> {
        // Define the regex pattern to extract the repository name
        private String patternString = "https://github\\.com/elastic/([^/]+)/.*";
        private Pattern pattern = Pattern.compile(patternString);

        @Override
        public PullRequest deserialize(com.google.gson.JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
            String htmlUrl = json.getAsJsonObject().get("html_url").getAsString();

            Type labelsListType = new TypeToken<List<Label>>() {
            }.getType();
            List<Label> labels = new Gson().fromJson(json.getAsJsonObject().get("labels").getAsJsonArray(), labelsListType);

            Matcher matcher = pattern.matcher(htmlUrl);
            matcher.find();
            return new PullRequest(
                matcher.group(1),
                json.getAsJsonObject().get("title").getAsString(),
                htmlUrl,
                json.getAsJsonObject().get("merged_at").getAsString(),
                labels
            );

        }
    }
}
