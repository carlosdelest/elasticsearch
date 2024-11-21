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

import java.util.List;

public class PullRequest {
    private final String repository;
    private final String title;
    private final String url;
    private final String mergedAt;
    private final List<Label> labels;

    public PullRequest(String repository, String title, String url, String mergedAt, List<Label> labels) {
        this.repository = repository;
        this.title = title;
        this.url = url;
        this.mergedAt = mergedAt;
        this.labels = labels;
    }

    public String getRepository() {
        return repository;
    }

    public String getTitle() {
        return title;
    }

    public String getUrl() {
        return url;
    }

    public String getMergedAt() {
        return mergedAt;
    }

    public List<Label> getLabels() {
        return labels;
    }

}
