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

package co.elastic.elasticsearch.settings.secure;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.common.Strings.format;
import static org.hamcrest.Matchers.is;

public class ReservedProjectSecretsActionTests extends ESTestCase {
    private TransformState<ProjectMetadata> processJSON(
        ReservedProjectSecretsAction action,
        TransformState<ProjectMetadata> prevState,
        String json
    ) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testValidation() {
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata state = ProjectMetadata.builder(projectId).build();
        TransformState<ProjectMetadata> prevState = new TransformState<>(state, Collections.emptySet());
        ReservedProjectSecretsAction action = new ReservedProjectSecretsAction();

        String json = """
            {
                "not_a_field": {"secure.test": "test"}
            }""";

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState, json)).getMessage(),
            is("[2:5] [project_secrets_parser] unknown field [not_a_field]")
        );
    }

    public void testSetAndOverwriteSettings() throws Exception {
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata state = ProjectMetadata.builder(projectId).build();
        TransformState<ProjectMetadata> prevState = new TransformState<>(state, Collections.emptySet());
        ReservedProjectSecretsAction action = new ReservedProjectSecretsAction();

        {
            String json = format("""
                {
                    "string_secrets": {"secure.test1": "test1"},
                    "file_secrets": {"secure.test2": "%s"}
                }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

            prevState = processJSON(action, prevState, json);
            ProjectSecrets projectSecrets = prevState.state().custom(ProjectSecrets.TYPE);
            assertEquals("test1", projectSecrets.getSettings().getString("secure.test1").toString());
            assertEquals("test2", new String(projectSecrets.getSettings().getFile("secure.test2").readAllBytes(), StandardCharsets.UTF_8));
        }
        {
            String json = format("""
                {
                    "string_secrets": {"secure.test1": "test3"},
                    "file_secrets": {"secure.test3": "%s"}
                }""", new String(Base64.getEncoder().encode("test4".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

            TransformState<ProjectMetadata> transformState = processJSON(action, prevState, json);
            ProjectSecrets projectSecrets = transformState.state().custom(ProjectSecrets.TYPE);
            assertEquals("test3", projectSecrets.getSettings().getString("secure.test1").toString());
            assertEquals("test4", new String(projectSecrets.getSettings().getFile("secure.test3").readAllBytes(), StandardCharsets.UTF_8));
            assertNull(projectSecrets.getSettings().getString("secure.test2"));
        }
    }

    public void testDuplicateSecretFails() {
        ProjectId projectId = randomUniqueProjectId();
        ProjectMetadata state = ProjectMetadata.builder(projectId).build();
        TransformState<ProjectMetadata> prevState = new TransformState<>(state, Collections.emptySet());
        ReservedProjectSecretsAction action = new ReservedProjectSecretsAction();

        String json = format("""
            {
                "string_secrets": {"secure.test1": "test1"},
                "file_secrets": {"secure.test1": "%s"}
            }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

        assertThat(
            expectThrows(XContentParseException.class, () -> processJSON(action, prevState, json)).getCause().getCause().getMessage(),
            is("Some settings were defined as both string and file settings: [secure.test1]")
        );

    }
}
