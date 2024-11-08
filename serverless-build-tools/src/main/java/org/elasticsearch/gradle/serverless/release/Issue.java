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

import java.time.Instant;
import java.util.List;

public class Issue {
    private final String title;
    private final String url;
    private final Instant createdAt;
    private final List<Label> labels;

    public Issue(String title, String url, String created_at, List<Label> labels) {
        this.title = title;
        this.url = url;
        createdAt = Instant.parse(created_at);
        this.labels = labels;
    }

    public String getTitle() {
        return title;
    }

    public String getUrl() {
        return url;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public List<Label> getLabels() {
        return labels;
    }


}
