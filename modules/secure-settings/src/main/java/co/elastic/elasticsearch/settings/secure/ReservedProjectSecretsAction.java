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

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
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

public class ReservedProjectSecretsAction implements ReservedClusterStateHandler<ProjectMetadata, SecureClusterStateSettings> {

    public static final ParseField STRING_SECRETS_FIELD = new ParseField("string_secrets");
    public static final ParseField FILE_SECRETS_FIELD = new ParseField("file_secrets");

    public static final String NAME = "project_secrets";

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
    public TransformState<ProjectMetadata> transform(
        SecureClusterStateSettings projectSecureSettings,
        TransformState<ProjectMetadata> prevState
    ) {
        ProjectMetadata projectMetadata = prevState.state();
        return new TransformState<>(
            ProjectMetadata.builder(projectMetadata).putCustom(ProjectSecrets.TYPE, new ProjectSecrets(projectSecureSettings)).build(),
            projectSecureSettings.getSettingNames()
        );
    }

    @Override
    public SecureClusterStateSettings fromXContent(XContentParser parser) {
        return new SecureClusterStateSettings(PARSER.apply(parser, null));
    }
}
