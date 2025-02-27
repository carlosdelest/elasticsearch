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
package co.elastic.elasticsearch.serverless.security.authc.saml;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.saml.SigningConfiguration;
import org.elasticsearch.xpack.security.authc.saml.SpConfiguration;
import org.opensaml.security.x509.X509Credential;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.serverless.security.authc.saml.MultiProjectSpSamlRealmSettings.SP_ACS;
import static co.elastic.elasticsearch.serverless.security.authc.saml.MultiProjectSpSamlRealmSettings.SP_ENTITY_ID;
import static co.elastic.elasticsearch.serverless.security.authc.saml.MultiProjectSpSamlRealmSettings.SP_LOGOUT;
import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.REQUESTED_AUTHN_CONTEXT_CLASS_REF;
import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.buildEncryptionCredential;
import static org.elasticsearch.xpack.security.authc.saml.SamlRealm.buildSigningConfiguration;

public class MultiProjectSamlSpConfiguration implements SpConfiguration {
    private final ProjectResolver projectResolver;
    private final SigningConfiguration signingConfiguration;
    private final List<String> reqAuthnCtxClassRef;
    private final List<X509Credential> encryptionCredentials;
    private final AtomicReference<Settings> settings = new AtomicReference<>();

    private MultiProjectSamlSpConfiguration(
        ProjectResolver projectResolver,
        SigningConfiguration signingConfiguration,
        List<X509Credential> encryptionCredentials,
        List<String> reqAuthnCtxClassRef,
        Settings settings
    ) {
        this.projectResolver = projectResolver;
        this.signingConfiguration = signingConfiguration;
        this.reqAuthnCtxClassRef = reqAuthnCtxClassRef;
        this.encryptionCredentials = encryptionCredentials;
        this.settings.set(settings);
    }

    public static MultiProjectSamlSpConfiguration create(
        ProjectResolver projectResolver,
        RealmConfig realmConfig,
        ClusterSettings clusterSettings
    ) throws GeneralSecurityException, IOException {
        List<X509Credential> encryptionCredential = buildEncryptionCredential(realmConfig);

        MultiProjectSamlSpConfiguration projectSamlServiceProviderConfiguration = new MultiProjectSamlSpConfiguration(
            projectResolver,
            buildSigningConfiguration(realmConfig),
            encryptionCredential != null ? encryptionCredential : Collections.<X509Credential>emptyList(),
            realmConfig.getSetting(REQUESTED_AUTHN_CONTEXT_CLASS_REF),
            getServiceProviderSettings(realmConfig)
        );

        clusterSettings.addAffixGroupUpdateConsumer(List.of(SP_ENTITY_ID, SP_ACS, SP_LOGOUT), (key, settings) -> {
            Settings.Builder settingsBuilder = Settings.builder();
            if (settings.keySet().isEmpty()) {
                settingsBuilder.put(projectSamlServiceProviderConfiguration.settings.get());
                settingsBuilder.remove(SP_ENTITY_ID.getConcreteSettingForNamespace(key).getKey());
                settingsBuilder.remove(SP_ACS.getConcreteSettingForNamespace(key).getKey());
                settingsBuilder.remove(SP_LOGOUT.getConcreteSettingForNamespace(key).getKey());
            } else {
                settingsBuilder.put(projectSamlServiceProviderConfiguration.settings.get());
                settingsBuilder.put(settings);
            }
            projectSamlServiceProviderConfiguration.settings.set(settingsBuilder.build());
        });
        return projectSamlServiceProviderConfiguration;
    }

    private static Settings getServiceProviderSettings(RealmConfig realmConfig) {
        Settings.Builder settingsBuilder = Settings.builder();
        Stream.of(SP_ENTITY_ID, SP_ACS, SP_LOGOUT)
            .flatMap(setting -> setting.getAllConcreteSettings(realmConfig.settings()))
            .forEach(s -> settingsBuilder.put(s.getKey(), s.get(realmConfig.settings())));
        return settingsBuilder.build();
    }

    private String require(Setting.AffixSetting<String> setting, String projectId) {
        Setting<String> projectScopedSetting = MultiProjectSpSamlRealmSettings.getProjectSetting(setting, projectId);
        String value = projectScopedSetting.get(settings.get());
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("The configuration setting [" + projectScopedSetting.getKey() + "] is required");
        }
        return value;
    }

    public String getEntityId() {
        return require(SP_ENTITY_ID, projectResolver.getProjectId().id());
    }

    public String getAscUrl() {
        return require(SP_ACS, projectResolver.getProjectId().id());
    }

    public String getLogoutUrl() {
        return require(SP_LOGOUT, projectResolver.getProjectId().id());
    }

    public List<X509Credential> getEncryptionCredentials() {
        return encryptionCredentials;
    }

    public SigningConfiguration getSigningConfiguration() {
        return signingConfiguration;
    }

    public List<String> getReqAuthnCtxClassRef() {
        return reqAuthnCtxClassRef;
    }
}
