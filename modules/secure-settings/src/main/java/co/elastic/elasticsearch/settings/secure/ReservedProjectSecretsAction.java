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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReservedProjectSecretsAction implements ReservedProjectStateHandler<ProjectSecrets> {
    public static final String NAME = "project_secrets";

    public static final ParseField STRING_SECRETS_FIELD = new ParseField("string_secrets");
    public static final ParseField FILE_SECRETS_FIELD = new ParseField("file_secrets");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Map<String, byte[]>, Void> PARSER = new ConstructingObjectParser<>(
        "project_secrets_parser",
        a -> {
            final var decoder = Base64.getDecoder();

            Map<String, byte[]> stringSecretsMap = a[0] == null
                ? Map.of()
                : ((Map<String, String>) a[0]).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getBytes(StandardCharsets.UTF_8)));

            Map<String, byte[]> fileSecretsByteMap = a[1] == null
                ? Map.of()
                : ((Map<String, String>) a[1]).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> decoder.decode(e.getValue())));

            Set<String> duplicateKeys = fileSecretsByteMap.keySet()
                .stream()
                .filter(stringSecretsMap::containsKey)
                .collect(Collectors.toSet());

            if (duplicateKeys.isEmpty() == false) {
                throw new IllegalStateException("Some settings were defined as both string and file settings: " + duplicateKeys);
            }

            return Stream.concat(stringSecretsMap.entrySet().stream(), fileSecretsByteMap.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), STRING_SECRETS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), FILE_SECRETS_FIELD);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransformState transform(ProjectId projectId, ProjectSecrets source, TransformState prevState) throws Exception {
        ClusterState clusterState = prevState.state();
        ProjectMetadata projectMetadata = clusterState.metadata().getProject(projectId);
        ProjectMetadata updatedMetadata = ProjectMetadata.builder(projectMetadata).putCustom(ProjectSecrets.TYPE, source).build();

        return new TransformState(
            ClusterState.builder(clusterState).putProjectMetadata(updatedMetadata).build(),
            source.getSettings().getSettingNames()
        );
    }

    @Override
    public ProjectSecrets fromXContent(XContentParser parser) {
        return new ProjectSecrets(new SecureClusterStateSettings(PARSER.apply(parser, null)));
    }
}
