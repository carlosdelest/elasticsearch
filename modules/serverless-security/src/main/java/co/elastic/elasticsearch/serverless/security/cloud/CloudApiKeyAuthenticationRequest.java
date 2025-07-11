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

package co.elastic.elasticsearch.serverless.security.cloud;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.CONTEXTS_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.PROJECT_ID_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.PROJECT_ORGANIZATION_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.PROJECT_TYPE_FIELD;
import static co.elastic.elasticsearch.serverless.security.cloud.CloudAuthenticationFields.TYPE_FIELD;

/**
 * Request to authenticate against serverless projects using a cloud API key.
 *
 * @param projects List of projects to authenticate against
 * @param cloudApiKey Cloud API key
 */
public record CloudApiKeyAuthenticationRequest(List<ProjectInfo> projects, CloudApiKey cloudApiKey) implements ToXContent {

    /**
     * The default context type when authenticating against serverless projects is "project".
     */
    private static final String CONTEXT_TYPE = "project";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(CONTEXTS_FIELD);
        for (ProjectInfo projectInfo : projects) {
            builder.startObject();
            builder.field(TYPE_FIELD, CONTEXT_TYPE);
            builder.field(PROJECT_ID_FIELD, projectInfo.projectId());
            builder.field(PROJECT_ORGANIZATION_FIELD, projectInfo.organizationId());
            builder.field(PROJECT_TYPE_FIELD, projectInfo.projectType());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{projects=" + projects + ", cloudApiKey=[::es_redacted::]}";
    }

}
