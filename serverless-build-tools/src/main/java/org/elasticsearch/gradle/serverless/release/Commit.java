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

public class Commit {
    private final String sha;
    private final String date;
    private final String message;

    public Commit(String sha, String date, String message) {
        this.sha = sha;
        this.date = date;
        this.message = message;
    }

    public String getSha() {
        return sha;
    }

    public String getDate() {
        return date;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "Commit{" +
            "sha='" + sha + '\'' +
            '}';
    }

    public String getShortMessage() {
        return message.split("\n")[0];
    }

}
